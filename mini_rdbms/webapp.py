# webapp.py
from flask import Flask, request, render_template, redirect
from rdbms import Database

app = Flask(__name__)
db = Database()

db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT UNIQUE)")

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        db.execute(
            f"INSERT INTO users VALUES ('{request.form['id']}', "
            f"'{request.form['name']}', '{request.form['email']}')"
        )
        return redirect("/")

    users = db.execute("SELECT * FROM users")
    return render_template("index.html", users=users)

@app.route("/delete/<id>")
def delete(id):
    db.execute(f"DELETE FROM users WHERE id = '{id}'")
    return redirect("/")

if __name__ == "__main__":
    app.run(debug=True)
