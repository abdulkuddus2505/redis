import redis
from redis.commands.search.field import TagField, VectorField, NumericField, TextField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
import csv
from traceback import print_stack


import redis

class Redis_Client():
    redis = None

    def __init__(self):
        self.redis = self.connect()

    """
    Connect to Redis with 'host', 'port', 'db', 'username' and 'password'.
    """
    def connect(self):
        try:
            # Establish the connection
            connection = redis.StrictRedis(
                host='redis-16258.c89.us-east-1-3.ec2.redns.redis-cloud.com',
                port=16258,
                password='VhUBDZbBv8KHPfNrfDe1pSmSk0Hg4Cri',
                decode_responses=True
            )
            print("Connection Successful!!!")
            return connection
        except Exception as e:
            print(f"Error connecting to Redis: {e}")
        finally:
            print("Connection attempt finished.")

    def get_connection(self):
        return self.redis

def create_index(redis_conn):
    """
    Create an index with secondary attributes for searching.
    """
    try:
        # Define the index schema
        index_definition = IndexDefinition(prefix=['user:'], index_type=IndexType.HASH)
        redis_conn.ft("user_idx").create_index([
            TextField('first_name'),
            TagField('gender'),
            TagField('country'),
            NumericField('latitude')
        ], definition=index_definition)
        print("Index created successfully!")
    except Exception as e:
        print(f"Error creating index: {e}")

def load_users(redis_conn, filepath):
    """
    Load users from a space-delimited file into Redis using batching for better performance.
    Args:
        redis_conn: Redis connection object.
        filepath: Path to the input file.
    Returns:
        int: The number of successfully loaded users.
    """
    print("Load data for user")
    result = 0

    with open(filepath, 'r') as file:
        pipeline = redis_conn.pipeline()
        
        for line_num, line in enumerate(file, start=1):
            fields = line.strip().split()
            fields = [field.strip('"') for field in fields]

            if len(fields) < 3 or len(fields) % 2 == 0:
                print(f"Skipping malformed line {line_num}: {line.strip()}")
                continue

            user_key = fields[0]
            user_data = {fields[i]: fields[i + 1] for i in range(1, len(fields), 2)}

            # Add hset to the pipeline
            pipeline.hset(user_key, mapping=user_data)
            result += 1

            # Execute pipeline every 1000 commands to avoid memory issues
            if result % 1000 == 0:
                pipeline.execute()

        # Execute remaining commands in the pipeline
        pipeline.execute()

    print(result)
    return result

    


def load_scores(redis_conn, filepath):
    """
    Load scores from scores.csv into Redis.
    Args:
        redis_conn: Redis connection object.
        filepath: Path to scores.csv file.
    """
    with open(filepath, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            user_id = row["user:id"]
            score = int(row["score"])
            leaderboard_id = row["leaderboard"]
            leaderboard_key = f"leaderboard:{leaderboard_id}"
            redis_conn.zadd(leaderboard_key, {user_id: score})


def query1(redis_conn, usr):
    """
    Fetches all attributes of a user.
    Args:
        redis_conn: Redis connection object.
        usr: User ID.
    Returns:
        Dictionary of user attributes.
    """
    key = f"user:{usr}"
    return redis_conn.hgetall(key)

def query2(redis_conn, usr):
    """
    Fetches longitude and latitude of a user.
    Args:
        redis_conn: Redis connection object.
        usr: User ID.
    Returns:
        Tuple of (longitude, latitude).
    """
    key = f"user:{usr}"
    longitude = redis_conn.hget(key, "longitude")
    latitude = redis_conn.hget(key, "latitude")
    return longitude, latitude


def query3(redis_conn):
    """
    Gets keys and last names of users whose IDs do not start with an odd number.
    Args:
        redis_conn: Redis connection object.
    Returns:
        List of tuples (key, last_name).
    """
    result = []

    # Initialize the pipeline
    pipeline = redis_conn.pipeline()

    # Use SCAN to iterate through keys in a memory-efficient way
    cursor = 0
    while True:
        cursor, keys = redis_conn.scan(cursor, match="user:*", count=100)  # Adjust count for batch size

        # Loop through the keys and add hget commands to the pipeline
        for key in keys:
            key_str = key if isinstance(key, str) else key.decode('utf-8')  # Decode if the key is bytes
            user_id = key_str.split(":")[1]  # Split and extract user ID
            
            if int(user_id[0]) % 2 == 0:  # Check if the user ID starts with an even number
                pipeline.hget(key_str, "last_name")  # Add hget command to the pipeline

        # Execute the pipeline if there are any commands
        if pipeline.command:
            last_names = pipeline.execute()

            # Construct the result: pair each key with its corresponding last_name
            result.extend([(key_str, last_names[i]) for i, key_str in enumerate(keys) if last_names[i] is not None])

        # If the cursor is 0, we have scanned all keys
        if cursor == 0:
            break

    return result



def query4(redis_conn):
    query = "@gender:{female} @country:{China|Russia} @latitude:[40 46]"

    try:
        result = redis_conn.ft("user_idx").search(query)
        
        for doc in result.docs:
            print(doc)

        return result.docs

    except Exception as e:
        print(f"Error executing query: {e}")
        return []

# def query5(redis_conn):
#     """
#     Fetches email IDs of the top 10 players in the leaderboard.
#     Args:
#         redis_conn: Redis connection object.
#     Returns:
#         List of email IDs.
#     """
#     leaderboard_key = "leaderboard"
#     leaderboard = redis_conn.zrange("leaderboard", 0, -1, withscores=True)
#     print(leaderboard)  # This will show the user IDs and their scores.

#     emails = []
#     for user_id in top_players:
#         key = f"user:{user_id}"
#         email = redis_conn.hget(key, "email")
#         emails.append(email)
#     return emails

def query5(redis_conn):
    """
    Fetches email IDs of the top 10 players from a sorted set leaderboard.
    Args:
        self.redis: Redis connection object.
    Returns:
        List of email IDs.
    """
    try:
        top_players = redis_conn.zrevrange("leaderboard:2", 0, 9)
        emails = []
        for user_id in top_players:
            email = redis_conn.hget(user_id, "email")
            if email:
                emails.append(email)

        return emails
    except Exception as e:
        print(f"Error querying top players: {e}")
        return []

redis_client = Redis_Client()
redis_conn = redis_client.get_connection()
pipeline = redis_conn.pipeline()

users_file = "users.txt"

# load_users(pipeline, users_file)
# load_scores(pipeline, "userscores.csv")

# user = redis_conn.hgetall("user:1")
# print(user)

# top_scorers = redis_conn.zrevrange("leaderboard:3", 0, 2, withscores=True)
# print(top_scorers)

# query1_result = query1(redis_conn, 299)
# print(query1_result)


# query2_result = query2(redis_conn, 2836)

# print(query2_result)


# query3_result = query3(redis_conn)
# print(query3_result)


# create_index(redis_conn)


# query4_result = query4(redis_conn)
# print(query4_result)


query5_result = query5(redis_conn)
print(query5_result)