
import logging
import collections
import logging.handlers
from typing import List

# # Attempt to import FastAPI and JSONResponse, handle if not available yet
# try:
#     from fastapi import FastAPI
#     from fastapi.responses import JSONResponse
# except ImportError:
#     print("FastAPI not yet available for import at this stage.")
#     FastAPI = None # Placeholder
#     JSONResponse = None # Placeholder

# # --- Configuration ---
# MAX_LOG_ENTRIES = 1000 # Limit the number of logs stored in memory

# # --- Global Deque for Logs ---
# # Needs to be accessible by both the handler and the endpoint function
# # Defined globally so it persists across setup calls if needed
# log_deque = collections.deque(maxlen=MAX_LOG_ENTRIES)

# # --- Custom Log Handler Definition ---
# class DequeLogHandler(logging.Handler):
#     """A custom logging handler that appends formatted records to a deque."""
#     def __init__(self, deque_instance):
#         super().__init__()
#         self.deque_instance = deque_instance

#     def emit(self, record):
#         # Avoid logging recursion if formatting/appending causes another log
#         if record.name == 'uvicorn.error': # Example: Avoid logging logger setup issues endlessly
#              # Maybe print directly or handle differently if needed
#              # print(f"Skipping uvicorn.error in DequeLogHandler: {record.getMessage()}")
#              return
#         try:
#             log_entry = self.format(record)
#             self.deque_instance.append(log_entry)
#         except Exception:
#             self.handleError(record)

# # --- Monkey-Patching Function ---
# def wrap_fastapi():
#     """
#     Applies monkey-patching to FastAPI.setup to add an in-memory log handler
#     and an endpoint (/papaya/logs) to retrieve recent logs.

#     MUST BE CALLED **BEFORE** `app = FastAPI()` is executed in your main script.

#     WARNING: Uses in-memory logging which is NOT recommended for production
#              due to memory usage and volatility (logs lost on restart). Max logs: {MAX_LOG_ENTRIES}.
#     WARNING: Exposing logs via an API endpoint has security risks. Implement
#              authentication/authorization if used beyond local debugging.
#     """
#     # Ensure FastAPI is available now
#     global FastAPI, JSONResponse
#     if FastAPI is None:
#         from fastapi import FastAPI
#     if JSONResponse is None:
#         from fastapi.responses import JSONResponse

#     print("Applying FastAPI patch for logging endpoint...")

#     # --- Configure Logging during Patch Application ---
#     # This setup runs once when wrap_fastapi() is called.
#     root_logger = logging.getLogger()
#     log_level = logging.INFO # Set desired capture level
#     # Set level on root logger ONLY if it's higher than desired,
#     # otherwise handlers control the level. Avoid lowering root level if already set higher.
#     if root_logger.level == logging.NOTSET or root_logger.level > log_level:
#          root_logger.setLevel(log_level)

#     log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
#     formatter = logging.Formatter(log_format)

#     # Configure and add our deque handler
#     deque_handler = DequeLogHandler(log_deque)
#     deque_handler.setLevel(log_level)
#     deque_handler.setFormatter(formatter)

#     # Add handler only if a handler of this type isn't already present
#     if not any(isinstance(h, DequeLogHandler) for h in root_logger.handlers):
#         root_logger.addHandler(deque_handler)
#         print(f"DequeLogHandler added to root logger (Level: {log_level}). Capturing max {MAX_LOG_ENTRIES} entries.")
#     else:
#         print("DequeLogHandler already configured on root logger.")

#     # --- Patch FastAPI.setup ---
#     # Capture original setup method *before* overwriting it
#     _old_setup = FastAPI.setup

#     def new_setup(self: FastAPI):
#         # Call original setup first (sets up docs URLs etc.)
#         _old_setup(self)
#         # print(f"Original FastAPI.setup() called for app: {self.title}") # Optional debug print

#         # Define the endpoint function *inside* new_setup
#         # It accesses the log_deque defined in the outer scope
#         async def get_logs(x) -> JSONResponse:
#             """Endpoint to retrieve recent logs stored in memory."""
#             # Convert deque to list for JSON serialization
#             # Return newest first if desired: list(reversed(log_deque))
#             return JSONResponse(content=list(log_deque))

#         # Add the /papaya/logs endpoint dynamically after setup
#         log_route_path = "/papaya/logs"

#         # Check if route already exists to prevent duplicates if setup is called multiple times
#         route_exists = any(hasattr(route, 'path') and route.path == log_route_path for route in self.routes)

#         if not route_exists:
#             try:
#                 self.add_route(
#                     log_route_path,
#                     get_logs, # <--- Pass the endpoint function positionally
#                     methods=["GET"],
#                     include_in_schema=False,
#                     name="get_papaya_logs"
#                 )
#                 print(f"Added log endpoint: {log_route_path}")
#             except Exception as e:
#                  print(f"Error adding log endpoint {log_route_path}: {e}")
#                  logging.exception(f"Error adding log endpoint {log_route_path}") # Log the error too

#         print("wrapping complete")
#         # else: # Optional debug print
#         #      print(f"Log endpoint {log_route_path} already exists.")

#     # Apply the patch - overwrite FastAPI.setup with our new version
#     FastAPI.setup = new_setup


# # code to start MCP server



# # code to set env variable

import os
import sys
import logging

# Basic logger setup (adjust as needed)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """
    Main function for the papaya command-line tool.
    Currently patches FastAPI and runs uvicorn.
    """

    print("at main")
    if len(sys.argv) < 2:
        print("Usage: papaya <command> [options]")
        print("Example: papaya uvicorn myapp:app --host 0.0.0.0 --port 8000")
        sys.exit(1)

    # Sets the environment variable to enable patching (if needed by the patch)
    os.environ["PAPAYA_PATCH_FASTAPI"] = "1"

    # Log the command being executed
    logger.info("Running command: %s", " ".join(sys.argv)) # Log the whole command including 'papaya'

    # For now, only allow the 'uvicorn' command
    if sys.argv[1] != "uvicorn":
        print(f"Error: Command '{sys.argv[1]}' not allowed.")
        print("Currently, only 'uvicorn' is supported.")
        print("Example: papaya uvicorn myapp:app --host 0.0.0.0 --port 8000")
        sys.exit(1) # Exit with error for invalid command

    # Check if there's at least an app location provided after 'uvicorn'
    if len(sys.argv) < 3:
        print("Error: Missing application location.")
        print("Example: papaya uvicorn myapp:app")
        sys.exit(1)

    # Import and apply the patch
    try:
        from papaya.wrappers import wrap_fastapi # Assuming papaya.patch exists
        wrap_fastapi()
        logger.info("fastapi patched successfully by papaya.")
    except ImportError:
        logger.warning("Could not import papaya.patch.patch_fastapi. Skipping patching.")
    except Exception as e:
        logger.error(f"Error applying patch: {e}")
        # Decide if you want to exit or continue without patching
        # sys.exit(1)

    try:
        import uvicorn

        # The application location (e.g., "myapp:app") is the first argument after 'uvicorn'
        app_loc = sys.argv[2]

        # Parse remaining arguments for uvicorn.run()
        # Note: uvicorn.run() primarily uses specific keyword arguments.
        # Passing arbitrary command-line flags directly might not work as expected
        # compared to running uvicorn from the command line itself.
        # This parser handles --key value and --flag patterns.
        kwargs = {}
        i = 3 # Start parsing from the argument after app_loc
        while i < len(sys.argv):
            arg = sys.argv[i]
            if arg.startswith('--'):
                key = arg[2:].replace('-', '_') # Convert --some-flag to some_flag

                # Check if it's a flag (no value follows or next arg is also a flag)
                if i + 1 >= len(sys.argv) or sys.argv[i + 1].startswith('-'):
                    kwargs[key] = True
                    i += 1
                # Otherwise, assume it's a key-value pair
                else:
                    value_str = sys.argv[i + 1]
                    # Attempt basic type conversions
                    try:
                        # Try int first
                        value = int(value_str)
                    except ValueError:
                        # Try boolean
                        if value_str.lower() == 'true':
                            value = True
                        elif value_str.lower() == 'false':
                            value = False
                        else:
                            # Keep as string if not int or bool
                            value = value_str

                    kwargs[key] = value
                    i += 2 # Move past both key and value
            else:
                # Handle positional arguments if necessary, or log warning/error
                logger.warning(f"Ignoring non-flag argument: {arg}")
                i += 1

        logger.info(f"Running uvicorn for '{app_loc}' with arguments: {kwargs}")

        # Run uvicorn programmatically
        # Common arguments for uvicorn.run: host, port, log_level, reload, workers, etc.
        uvicorn.run(app_loc, **kwargs)

    except ImportError:
        logger.error("Fatal: Could not import uvicorn. Please install it: pip install uvicorn")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal: Error running uvicorn directly: {e}")
        # Consider printing traceback for debugging
        # import traceback
        # traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
