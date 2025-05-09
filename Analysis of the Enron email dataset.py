#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 22:00:09 2020

@author: ufac001
"""

from email.parser import Parser
import re
import time
from datetime import datetime, timezone, timedelta


def utf8_decode_and_filter(rdd):
    def utf_decode(s):
        try:
            return str(s, 'utf-8')
        except:
            pass
    return rdd.map(lambda x: utf_decode(x[1])).filter(lambda x: x != None)


    
def date_to_dt(date):
    def to_dt(tms):
        def tz():
            return timezone(timedelta(seconds=tms.tm_gmtoff))
        return datetime(tms.tm_year, tms.tm_mon, tms.tm_mday, 
                      tms.tm_hour, tms.tm_min, tms.tm_sec, 
                      tzinfo=tz())
    return to_dt(time.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S %z'))

# Q1: replace pass with your code
def extract_email_network(rdd):
    """
    Extract email network from an RDD of email messages.
    
    Args:
        rdd: An RDD of email messages as strings (already decoded using utf8_decode_and_filter)
    
    Returns:
        An RDD of triples (sender, recipient, timestamp) representing the email network
    """
    
    # Define a regex pattern to validate email format
    # This matches standard email format with username@domain.tld
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    # Helper function to check if an email belongs to Enron domain
    # Returns True only if domain ends with enron.com
    def is_enron_email(email):
        if not email:
            return False
        parts = email.lower().split('@')  # Split at @ to separate username and domain
        if len(parts) != 2:
            return False
        domain_parts = parts[1].split('.')  # Split domain by periods
        # Check if domain has at least two parts and ends with enron.com
        return len(domain_parts) >= 2 and domain_parts[-2] == 'enron' and domain_parts[-1] == 'com'
    
    # First transformation: Parse each raw email string into an email object
    # using Python's email parser to access email headers
    rdd_mail = rdd.map(lambda x: Parser().parsestr(x))
    
    # Extract the three key fields needed for our network: sender, recipients, timestamp
    def extract_fields(email):
        sender = email.get('From')  # Extract sender from From field
        
        # Extract all recipients from To, Cc, and Bcc fields
        recipients = []
        for field in ['To', 'Cc', 'Bcc']:
            if email.get(field):
                # Split multiple recipients on commas and clean up whitespace
                field_recipients = [addr.strip() for addr in email.get(field).split(',')]
                recipients.extend(field_recipients)
        
        # Convert string timestamp to datetime object with timezone
        timestamp = None
        if email.get('Date'):
            try:
                timestamp = date_to_dt(email.get('Date'))  # Convert using provided helper function
            except:
                pass  # Skip emails with invalid date formats
                
        return (sender, recipients, timestamp)
    
    # Apply the extraction function to each email
    rdd_fields = rdd_mail.map(extract_fields)
    
    # Convert the (sender, [recipients], timestamp) format to multiple (sender, recipient, timestamp) triples
    def create_triples(fields):
        sender, recipients, timestamp = fields
        # Skip entries with missing critical fields
        if not sender or not recipients or not timestamp:
            return []
        
        # Create one triple for each recipient of this email
        return [(sender, recipient, timestamp) for recipient in recipients]
    
    # Use flatMap to convert each email into multiple triples, one per recipient
    rdd_triples = rdd_fields.flatMap(create_triples)
    
    # Apply the required filters in sequence:
    rdd_filtered = rdd_triples.filter(
        lambda x: x[0] != x[1]  # 1. Remove self-loops (emails sent to oneself)
    ).filter(
        lambda x: re.match(email_regex, x[0]) and re.match(email_regex, x[1])  # 2. Ensure valid email format
    ).filter(
        lambda x: is_enron_email(x[0]) and is_enron_email(x[1])  # 3. Keep only Enron domain emails
    )
    
    # Remove any duplicate triples that might exist
    rdd_distinct = rdd_filtered.distinct()
    
    return rdd_distinct


# Q2: replace pass with your code
def convert_to_weighted_network(rdd, drange=None):
    """
    Convert an email network RDD to a weighted network.
    
    Args:
        rdd: An RDD of triples (sender, recipient, timestamp) from extract_email_network()
        drange: Optional tuple (d1, d2) of datetime objects defining a time range
                Both d1 and d2 should have timezone.utc as tzinfo
                
    Returns:
        An RDD of triples (origin, destination, weight) where weight is the 
        number of emails sent from origin to destination within the specified date range
    """
    # If a date range is provided, filter the RDD to include only emails within that range
    # This reduces the dataset before performing the expensive counting operation
    if drange is not None:
        start_date, end_date = drange
        filtered_rdd = rdd.filter(lambda x: start_date <= x[2] <= end_date)
    else:
        # If no date range specified, use the entire RDD
        filtered_rdd = rdd
    
    # Transform each triple (sender, recipient, timestamp) into a key-value pair format
    # where key is (sender, recipient) tuple and value is 1 (representing one email)
    # This prepares the data for counting emails between each unique sender-recipient pair
    sender_recipient_pairs = filtered_rdd.map(lambda x: ((x[0], x[1]), 1))
    
    # Use reduceByKey to sum up all the 1s for each unique (sender, recipient) pair
    # This efficiently counts the number of emails sent from each sender to each recipient
    weighted_edges = sender_recipient_pairs.reduceByKey(lambda a, b: a + b)
    
    # Transform the data back into the required output format (origin, destination, weight)
    # This unpacks the tuple key into separate fields and keeps the count as the weight
    result = weighted_edges.map(lambda x: (x[0][0], x[0][1], x[1]))
    
    return result




# Q3.1: replace pass with your code
def get_out_degrees(weighted_network_rdd):
    """
    Compute the weighted out-degree for each node in a weighted network.
    
    Args:
        weighted_network_rdd: An RDD of triples (origin, destination, weight) 
                             from convert_to_weighted_network()
    
    Returns:
        An RDD of pairs (degree, node) sorted in descending lexicographical order
    """
    # Extract origin-weight pairs from each edge, discarding the destination
    # This focuses on the outgoing edges from each node
    origin_weight_pairs = weighted_network_rdd.map(lambda x: (x[0], x[2]))
    
    # Sum the weights for each origin node to calculate its total out-degree
    # reduceByKey combines all values for the same origin, summing the weights
    out_degrees = origin_weight_pairs.reduceByKey(lambda a, b: a + b)
    
    # Collect all nodes in the network to ensure we account for nodes with zero out-degree
    # First, get all nodes that appear as origins (senders)
    all_origins = weighted_network_rdd.map(lambda x: x[0])
    # Then, get all nodes that appear as destinations (recipients)
    all_destinations = weighted_network_rdd.map(lambda x: x[1])
    # Combine both sets and remove duplicates to get the complete set of nodes
    all_nodes = all_origins.union(all_destinations).distinct()
    
    # Create a default entry with out-degree 0 for each node
    # This ensures nodes with no outgoing edges will still appear in the results
    default_degrees = all_nodes.map(lambda node: (node, 0))
    
    # Merge the actual out-degrees with the default degrees
    # fullOuterJoin combines both RDDs, preserving all keys from both sides
    # For each node, we take its actual out-degree if it exists, otherwise use 0
    combined_degrees = out_degrees.fullOuterJoin(default_degrees).map(
        lambda x: (x[0], x[1][0] if x[1][0] is not None else x[1][1])
    )
    
    # Swap key-value positions to get (degree, node) format and sort
    # Sort in descending order of degree, and for nodes with the same degree,
    # sort lexicographically by node name
    return combined_degrees.map(lambda x: (x[1], x[0])).sortBy(lambda x: (x[0],x[1]), ascending=False)



# Q3.2: replace pass with your code         
def get_in_degrees(weighted_network_rdd):
   """
   Compute the weighted in-degree for each node in a weighted network.
   
   Args:
       weighted_network_rdd: An RDD of triples (origin, destination, weight) 
                            from convert_to_weighted_network()
   
   Returns:
       An RDD of pairs (degree, node) sorted in descending lexicographical order
   """
   # Extract destination-weight pairs from each edge, focusing on incoming connections
   # This keeps track of emails received by each node (destination) and their weights
   destination_weight_pairs = weighted_network_rdd.map(lambda x: (x[1], x[2]))
   
   # Sum the weights for each destination node to calculate its total in-degree
   # This aggregates all incoming email weights for each recipient
   in_degrees = destination_weight_pairs.reduceByKey(lambda a, b: a + b)
   
   # Identify all nodes in the network to ensure we include those with zero in-degree
   # First collect all sender nodes (origins)
   all_origins = weighted_network_rdd.map(lambda x: x[0])
   # Then collect all recipient nodes (destinations)
   all_destinations = weighted_network_rdd.map(lambda x: x[1])
   # Combine both sets and remove duplicates to get the complete set of nodes in the network
   all_nodes = all_origins.union(all_destinations).distinct()
   
   # Create a placeholder with in-degree 0 for each node in the network
   # This ensures we account for nodes that don't receive any emails
   default_degrees = all_nodes.map(lambda node: (node, 0))
   
   # Merge the actual in-degrees with the default degrees using a full outer join
   # For each node, we take its actual in-degree if it exists, otherwise use 0
   # This ensures every node in the network appears in the result, even those with no incoming emails
   combined_degrees = in_degrees.fullOuterJoin(default_degrees).map(
       lambda x: (x[0], x[1][0] if x[1][0] is not None else x[1][1])
   )
   
   # Transform to (degree, node) format and sort in descending order
   # The sort criteria are first by degree (highest first), then by node name
   return combined_degrees.map(lambda x: (x[1], x[0])).sortBy(lambda x: (x[0],x[1]), ascending=False)



# Q4.1: replace pass with your code            
def get_out_degree_dist(weighted_network_rdd):
   """
   Compute the distribution of weighted out-degrees for a weighted network.
   
   Args:
       weighted_network_rdd: An RDD of triples (origin, destination, weight)
                            from convert_to_weighted_network()
   
   Returns:
       An RDD of pairs (degree, count) sorted in ascending order of degree,
       where count is the number of nodes with that out-degree
   """
   # First extract origin-weight pairs to focus on outgoing connections
   # For each edge, we keep the sender node (origin) and the edge weight
   origin_weight_pairs = weighted_network_rdd.map(lambda x: (x[0], x[2]))
   
   # Sum up all weights for each origin node to calculate total out-degrees
   # This gives us the total weight of emails sent by each node
   out_degrees = origin_weight_pairs.reduceByKey(lambda a, b: a + b)
   
   # Collect all nodes in the network to ensure completeness
   # We need both senders and recipients to include all possible network participants
   all_origins = weighted_network_rdd.map(lambda x: x[0])
   all_destinations = weighted_network_rdd.map(lambda x: x[1])

   # Union merges all_origin and all_destinations
   # Distinct removes duplicates to get the complete set of unique nodes
   all_nodes = all_origins.union(all_destinations).distinct()
   
   # Create a baseline of zero out-degree for all nodes
   # This ensures nodes that never send emails are still counted
   default_degrees = all_nodes.map(lambda node: (node, 0))
   
   # Merge the actual out-degrees with the default degrees
   # fullOuterJoin combines both RDDs, preserving all keys from both sides
   # For each node, we take its actual out-degree if it exists, otherwise use 0
   combined_degrees = out_degrees.fullOuterJoin(default_degrees).map(
       lambda x: (x[0], x[1][0] if x[1][0] is not None else x[1][1])
   )
   
   # Convert to degree distribution by counting nodes with each degree value
   # Map each node to (degree, 1) then reduce by key to count occurrences of each degree
   degree_counts = combined_degrees.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)
   
   # Sort the distribution by degree in ascending order
   # This gives a clear view of how out-degrees are distributed across the network
   return degree_counts.sortByKey(ascending=True)



# Q4.2: replace pass with your code
def get_in_degree_dist(weighted_network_rdd):
   """
   Compute the distribution of weighted in-degrees for a weighted network.
   
   Args:
       weighted_network_rdd: An RDD of triples (origin, destination, weight)
                            from convert_to_weighted_network()
   
   Returns:
       An RDD of pairs (degree, count) sorted in ascending order of degree,
       where count is the number of nodes with that in-degree
   """
   # Extract destination-weight pairs to focus on incoming connections
   # This isolates the recipient (destination) and weight for each email communication
   destination_weight_pairs = weighted_network_rdd.map(lambda x: (x[1], x[2]))
   
   # Calculate the total in-degree for each node by summing weights of incoming edges
   # This aggregates how many weighted emails each person received
   in_degrees = destination_weight_pairs.reduceByKey(lambda a, b: a + b)
   
   # Gather all unique nodes in the network (both senders and recipients)
   # This ensures we don't miss any nodes that might only send or only receive
   all_origins = weighted_network_rdd.map(lambda x: x[0])
   all_destinations = weighted_network_rdd.map(lambda x: x[1])
   all_nodes = all_origins.union(all_destinations).distinct()
   
   # Establish baseline of zero in-degree for all nodes in the network
   # This accounts for nodes that never receive any emails
   default_degrees = all_nodes.map(lambda node: (node, 0))
   
   # Merge the actual out-degrees with the default degrees
   # fullOuterJoin combines both RDDs, preserving all keys from both sides
   # For each node, we take its actual out-degree if it exists, otherwise use 0
   combined_degrees = in_degrees.fullOuterJoin(default_degrees).map(
       lambda x: (x[0], x[1][0] if x[1][0] is not None else x[1][1])
   )
   
   # Calculate distribution by counting how many nodes have each degree value
   # Map each node to (degree, 1) then reduce to count frequency of each degree
   degree_counts = combined_degrees.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)
   
   # Organize the distribution by sorting on degree values in ascending order
   # This creates a clear picture of how in-degrees are distributed across the network
   return degree_counts.sortByKey(ascending=True)




# Q5: replace pass with your code
def get_monthly_contacts(rdd):
   """
   For each sender, identify the month when they contacted the most unique people.
   
   Args:
       rdd: An RDD of triples (sender, recipient, timestamp) from extract_email_network()
   
   Returns:
       An RDD of triples (sender, month, count) sorted in descending order by count first, then sender,
       where month is in MM/YYYY format and count is the number of distinct recipients in that month
   """
   # Transform each record to extract the month information from the timestamp
   # This converts the full datetime into a standardized month/year format
   def extract_month(record):
       sender, recipient, timestamp = record
       # Format timestamp into MM/YYYY format for consistent monthly aggregation
       month_str = f"{timestamp.month}/{timestamp.year}"
       return (sender, recipient, month_str)
   
   # Apply the transformation to get (sender, recipient, month) tuples
   sender_recipient_month = rdd.map(extract_month)
   
   # Restructure data to group by sender and month
   # Create composite key (sender, month) with recipient as value for easier aggregation
   sender_month_recipient = sender_recipient_month.map(lambda x: ((x[0], x[2]), x[1]))
   
   # Remove duplicate entries to count each recipient only once per sender per month
   # This ensures we're counting unique people contacted, not total emails sent
   distinct_recipients = sender_month_recipient.distinct()
   
   # Count the number of unique recipients for each sender-month pair
   # First group all recipients for each (sender, month) combination
   # Then count how many unique recipients each sender contacted in each month
   monthly_contact_counts = distinct_recipients.groupByKey().mapValues(lambda recipients: len(set(recipients)))
   
   # Restructure data to prepare for finding the max month per sender
   # Transform from ((sender, month), count) to (sender, (month, count))
   sender_month_count = monthly_contact_counts.map(lambda x: (x[0][0], (x[0][1], x[1])))
   
   # For each sender, find the month(s) with the highest number of contacts
   # This function handles cases where multiple months might tie for maximum
   def find_max_month(data):
       sender, month_counts = data
       max_count = 0
       max_months = []
       
       # Iterate through all months for this sender to find the maximum
       for month, count in month_counts:
           if count > max_count:
               # Found a new maximum - reset the list and update max count
               max_count = count
               max_months = [(month, count)]
           elif count == max_count:
               # Found a tie for maximum - add to the list of maximum months
               max_months.append((month, count))
       
       # Return all months that tied for the maximum as separate records
       return [(sender, month, count) for month, count in max_months]
   
   # Apply the maximum finding logic to each sender
   max_contact_months = sender_month_count.groupByKey().flatMap(find_max_month)
   
   # Sort the results as specified: first by count (descending), then by sender
   # The negative sign on count creates a descending order
   result = max_contact_months.sortBy(lambda x: (-x[2], x[0]))
   
   return result