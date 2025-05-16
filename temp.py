import tracemalloc
from twikit import Client
import asyncio
from random import randint

# Enable tracemalloc to track memory allocations
tracemalloc.start()

def load_credentials():
    """Load Twitter credentials from dictionary"""
    return {
        "username": "HOMINH10751",
        "email": "minhaloalo1236@gmail.com",
        "password": "TThUjVDu%Ac8n_v",
    }

async def initialize_client(credentials):
    """Initialize and authenticate Twitter client"""
    client = Client(language='en-US')
    await client.login(
        auth_info_1=credentials["username"], 
        auth_info_2=credentials["email"], 
        password=credentials["password"]
    )
    client.save_cookies('cookies.json')
    return client

async def main():
    """Main function to run the Twitter client"""
    # Load credentials and initialize client
    credentials = load_credentials()
    client = await initialize_client(credentials)
    
    # Add any additional operations with the client here
    
    return client

if __name__ == "__main__":
    # Use asyncio.run to properly run the async main function
    asyncio.run(main())