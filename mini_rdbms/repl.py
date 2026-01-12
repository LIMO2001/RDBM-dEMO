# repl.py
from rdbms import Database

db = Database()
print("MiniRDBMS REPL (type 'exit' to quit)")

while True:
    try:
        cmd = input("db> ")
        if cmd.lower() == "exit":
            break
        result = db.execute(cmd)
        if isinstance(result, list):
            for r in result:
                print(r)
        else:
            print(result)
    except Exception as e:
        print("Error:", e)
