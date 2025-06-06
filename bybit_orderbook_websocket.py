import time
import json
from pybit.unified_trading import WebSocket

# ==============================================================================
# --- Configuration ---
# ==============================================================================

# -- Connection Settings --
# The symbol you want to stream data for (e.g., "BTCUSDT", "ETHUSDT").
SYMBOL = "BTCUSDT"
# The channel type for the data stream. Use "spot" for Spot trading.
CHANNEL_TYPE = "spot"
# The order book depth. Options include 1, 50, 200, 500 for Spot.
DEPTH = 50

# -- Data Collection Settings --
DURATION_SECONDS = 3600
# The name of the output file where the data will be saved.
OUTPUT_FILENAME = f"bybit_{SYMBOL.lower()}_{CHANNEL_TYPE}_orderbook_{DEPTH}_1hr.json"

# ==============================================================================
# --- Global variables ---
# ==============================================================================

collected_messages = []
first_message_printed = False # Flag to ensure we only print the first sample once

# ==============================================================================
# --- WebSocket Callback Function ---
# ==============================================================================

def handle_orderbook_message(message: dict):
    """
    This function is called for every message received from the WebSocket.
    It appends the raw message to a global list, prints the first message received,
    and prints a status update to the console every 1000 messages.
    """
    global collected_messages, first_message_printed
    collected_messages.append(message)

    # Print the very first message received as a sample
    if not first_message_printed:
        print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] First WebSocket message received (sample):")
        print(json.dumps(message, indent=2))
        print("-" * 50) # Separator
        first_message_printed = True

    # To avoid flooding the console, print a status update every 1000 messages collected after the first.
    if len(collected_messages) % 1000 == 0:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Messages collected: {len(collected_messages):,}")

# ==============================================================================
# --- Main Script Execution ---
# ==============================================================================

if __name__ == "__main__":
    print("Starting Bybit WebSocket data collector...")
    print(f"Configuration: Symbol={SYMBOL}, Depth={DEPTH}, Channel={CHANNEL_TYPE}")
    print(f"The script will run for {DURATION_SECONDS / 60:.0f} minutes ({DURATION_SECONDS} seconds).")
    print(f"Collected data will be saved to: {OUTPUT_FILENAME}")

    # Initialize the WebSocket client for the specified channel type.
    ws = WebSocket(
        testnet=False,  # Set to True for Bybit testnet
        channel_type=CHANNEL_TYPE,
    )

    # Subscribe to the order book stream using the configured parameters
    # and the callback function to handle messages.
    print(f"Subscribing to orderbook.{DEPTH}.{SYMBOL}...")
    ws.orderbook_stream(
        depth=DEPTH,
        symbol=SYMBOL,
        callback=handle_orderbook_message
    )

    # Keep the script running for the specified duration.
    start_time = time.time()
    print(f"Data collection started at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
    try:
        while time.time() - start_time < DURATION_SECONDS:
            # The WebSocket runs in a background thread, so we just need to
            # keep the main thread alive.
            time.sleep(1)
            # Check if the WebSocket is still connected.
            if not ws.is_connected():
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] WebSocket is not connected! Attempting to exit and save data.")
                break
        
        if time.time() - start_time >= DURATION_SECONDS:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Specified duration of {DURATION_SECONDS} seconds reached.")

    except KeyboardInterrupt:
        # Allows the user to stop the script manually with Ctrl+C.
        print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Script interrupted by user. Proceeding to save data...")

    finally:
        # Ensure the WebSocket connection is closed cleanly.
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Closing WebSocket connection...")
        if ws.is_connected(): # Check if it's connected before trying to exit
            ws.exit()
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] WebSocket connection closed.")

    # After the loop finishes or is interrupted, save the collected data.
    if collected_messages:
        print(f"\n--- Data Collection Summary ---")
        print(f"Collected a total of {len(collected_messages):,} messages.")
        
        # The first message was already printed during collection if received.
        # We can still print the first and last from the collected_messages list for final summary.
        if not first_message_printed and collected_messages: # If somehow it was missed (e.g. very short run)
            print("\nFirst message from collected list (if not printed during stream):")
            print(json.dumps(collected_messages[0], indent=2))

        if len(collected_messages) > 1:
            print("\nLast message collected (from list):")
            print(json.dumps(collected_messages[-1], indent=2))
        elif collected_messages and first_message_printed: # If only one message was collected and already printed
             print("\nOnly one message was collected (already printed as first sample).")


        print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Saving data to '{OUTPUT_FILENAME}'...")
        try:
            with open(OUTPUT_FILENAME, 'w') as f:
                # Use json.dump to write the list of messages to the file.
                # indent=2 makes the JSON file readable.
                json.dump(collected_messages, f, indent=2)
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Data successfully saved to {OUTPUT_FILENAME}. ✅")
        except Exception as e:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Error: Failed to save data to JSON file: {e}")
    else:
        print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] No messages were collected.")
    
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Script finished.")
