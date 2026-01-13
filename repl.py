import sys
import os

# Ensure we can import from the current directory
sys.path.append(os.getcwd())

from core import Database, DBError
from parser import SQLParser

def start_repl():
    db = Database()
    # Attempt to load existing data
    if os.path.exists('pesapal.json'):
        try:
            db.load()
            print("Loaded persisted data.")
        except:
            print("Could not load data, starting fresh.")
            
    parser = SQLParser(db)

    print("Welcome to Pesapal RDBMS. Type HELP for commands. Type EXIT or CTRL+C to quit.")
    
    while True:
        try:
            user_input = input("pesapal-db > ")
            if not user_input:
                continue
            
            if user_input.strip().upper() == 'EXIT':
                break
                
            result = parser.execute(user_input)
            print(result)
            
            # Auto-save after every command for safety
            db.save()
            
        except DBError as e:
            print(f"Error: {e}")
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"System Error: {e}")

if __name__ == "__main__":
    start_repl()