from app import app
from flask import render_template, jsonify
from flask import request

#--------------------------------------------

#   DESHBOARD PAGE 

@app.route('/')
def home():



    return render_template('index.html')

# -------------------------------------------------

