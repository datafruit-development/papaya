from fastmcp import FastMCP
import http.client

server_endpoint = "localhost"
server_port = 8000

mcp = FastMCP("Papaya")

@mcp.tool()
def get_logs() -> str: # Changed return type hint to str, as logs are usually text
    """ Get the logs from the webserver """
    conn = None
    try:
        # Establish connection (use HTTPSConnection for https)
        # Added a timeout for robustness
        conn = http.client.HTTPConnection(server_endpoint, server_port, timeout=10)

        # Send GET request
        conn.request("GET", "/papaya/logs")

        # Get the response
        response = conn.getresponse()

        # Read the response data
        data = response.read()

        # Check if the request was successful (status code 200)
        if response.status == 200:
            # Decode bytes to string (assuming UTF-8 encoding for logs)
            logs_content = data.decode('utf-8')
            return logs_content
        else:
            # Return an error message if status code is not 200
            error_message = f"Error fetching logs: {response.status} {response.reason}\nResponse body: {data.decode('utf-8', errors='ignore')}"
            print(error_message) # Also print for server-side visibility
            return error_message

    except Exception as e:
        # Catch any other unexpected errors
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        return error_message
    finally:
        # Ensure the connection is closed
        if conn:
            conn.close()


# from fastmcp import FastMCP

# # Create an MCP server
# mcp = FastMCP("Demo")

# # Add an addition tool
# @mcp.tool()
# def add(a: int, b: int) -> int:
#     """Add two numbers"""
#     return a + b

# # Add a dynamic greeting resource
# @mcp.resource("greeting://{name}")
# def get_greeting(name: str) -> str:
#     """Get a personalized greeting"""
#     return f"Hello, {name}!"


if __name__ == "__main__":
    print(get_logs())
