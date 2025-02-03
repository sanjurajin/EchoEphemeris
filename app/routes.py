from app import app
from flask import render_template, jsonify
from flask import request

#--------------------------------------------

#   DESHBOARD PAGE 

from app.id_utils import zx_64439cbd , x0f7092b4 
@app.route('/')
def home():
    charts_data = x0f7092b4()

    # x, y = equity_curve()
    eq_dates, equity = zx_64439cbd()

    return render_template('index.html',charts_data=charts_data, eq_dates=eq_dates, equity=equity)

# -------------------------------------------------

