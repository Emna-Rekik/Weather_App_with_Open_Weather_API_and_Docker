from flask import Flask, render_template, request, url_for, redirect, session,request
from pymongo import MongoClient
from datetime import datetime, timedelta
import bcrypt
import datetime
import requests
import time
import json
from datetime import datetime

#docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' open_weather_app_db_1
#set app as a Flask instance 
app = Flask(__name__)
#encryption relies on secret keys so they could be run
app.secret_key = "testing"

#IP = '172.21.0.2'
IP = 'test_mongodb'

# #connect to your Mongo DB database
def MongoDB():
    client = MongoClient("mongodb+srv://db:pw@cluster0-xth9g.mongodb.net/Richard?retryWrites=true&w=majority")
    db = client.get_database('total_records')
    records = db.register
    return records
#records = MongoDB()


##Connect with Docker Image###
def dockerMongoDB():
    client = MongoClient(host=IP,
                            port=27017, 
                            username='root', 
                            password='pass',
                            authSource="admin")
    db = client.users
    records = db.register
    """
    pw = "test123"
    hashed = bcrypt.hashpw(pw.encode('utf-8'), bcrypt.gensalt())
    records = db.register
    records.insert_one({
        "name": "Test Test",
        "email": "test@yahoo.com",
        "password": hashed
    })
    """
    return records

records = dockerMongoDB()


#assign URLs to have a particular route 
@app.route("/", methods=['post', 'get'])
def index():
    message = ''
    #if method post in index
    if "email" in session:
        return redirect(url_for("home"))
    if request.method == "POST":
        user = request.form.get("fullname")
        email = request.form.get("email")
        password1 = request.form.get("password1")
        password2 = request.form.get("password2")
        #if found in database showcase that it's found 
        user_found = records.find_one({"name": user})
        email_found = records.find_one({"email": email})
        if user_found:
            message = 'There already is a user by that name'
            return render_template('index.html', message=message)
        if email_found:
            message = 'This email already exists in database'
            return render_template('index.html', message=message)
        if password1 != password2:
            message = 'Passwords should match!'
            return render_template('index.html', message=message)
        else:
            #hash the password and encode it
            hashed = bcrypt.hashpw(password2.encode('utf-8'), bcrypt.gensalt())
            #assing them in a dictionary in key value pairs
            Country = request.form.get("Country")
            user_input = {'name': user, 'email': email, 'password': hashed, 'Country': Country}
            #insert it in the record collection
            records.insert_one(user_input)
            
            #find the new created account and its email
            user_data = records.find_one({"email": email})
            new_email = user_data['email']
            #if registered redirect to logged in as the registered user
            return redirect(url_for("home"))
    return render_template('index.html')
    
@app.route('/historique')
def historique():
    client = MongoClient(host=IP,
                            port=27017, 
                            username='root', 
                            password='pass',
                            authSource="admin")
    db = client.users
    records = db.weather_collection.find({"email": session["email"]})
    
    # Compute the date 15 days ago
    date_15_days_ago = datetime.now() - timedelta(days=10)
    
    # Convert date to string
    date_15_days_ago_str = date_15_days_ago.strftime("%A %B, %d %Y %H:%M:%S")
    
    # Query the database for records from the last 15 days
    records = db.weather_collection.find({"email": session["email"], "Time": {"$gte": date_15_days_ago_str}})
    
    return render_template('historique.html', records = records)

@app.route("/login", methods=["POST", "GET"])
def login():
    message = 'Please login to your account'
    if "email" in session:
        return redirect(url_for("home"))

    if request.method == "POST":
        email = request.form.get("email")
        password = request.form.get("password")

        #check if email exists in database
        email_found = records.find_one({"email": email})
        if email_found:
            email_val = email_found['email']
            passwordcheck = email_found['password']
            #encode the password and check if it matches
            if bcrypt.checkpw(password.encode('utf-8'), passwordcheck):
                session["email"] = email_val
                return redirect(url_for('home'))
            else:
                if "email" in session:
                    return redirect(url_for("home"))
                message = 'Wrong password'
                return render_template('login.html', message=message)
        else:
            message = 'Email not found'
            return render_template('login.html', message=message)
    return render_template('login.html', message=message)
    
@app.route('/prediction', methods=["POST", "GET"])
def prediction():
    API_KEY = '94d3b2c21850c2a5da8ad81d24b4480e'
    
    CITY = request.args.get('city')
    print(CITY)
    API_URL = f'http://api.openweathermap.org/data/2.5/forecast?q={CITY}&appid={API_KEY}'

    response = requests.get(API_URL)

    forecasts = []
    if response.status_code == 200:
        data = json.loads(response.text)
        for forecast in data['list']:
            date_time = forecast['dt_txt']
            temperature = int(forecast['main']['temp']-273.15)
            weather = forecast['weather'][0]['description']
            forecasts.append((date_time, temperature, weather))
    else:
        print('Error:', response.status_code, response.text)

    return render_template('prediction.html', forecasts=forecasts, CITY=CITY)
        

@app.route('/home', methods=['GET', 'POST'])
def home():
    client = MongoClient(host=IP,
                            port=27017, 
                            username='root', 
                            password='pass',
                            authSource="admin")
    db = client.users
    records = db.register
    user_data = records.find_one({"email": session["email"]})
    email = user_data["email"]
    username = user_data['name'].split()[0]
    if request.method == 'POST':
        city_name = request.form.get('city')

        #take a variable to show the json data
        #r = requests.get('https://api.openweathermap.org/data/2.5/weather?q='+city_name+'&appid=4ae90cba059dcd874713e3fead265b7f')
        
        try:
            r = requests.get('https://api.openweathermap.org/data/2.5/weather?q='+city_name+'&appid=4ae90cba059dcd874713e3fead265b7f')
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            sweetalert=True
            return render_template('home.html',sweetalert=sweetalert)

        #read the json object
        json_object = r.json()

        #take some attributes like temperature,humidity,pressure of this 
        temperature = int(json_object['main']['temp']-273.15) #this temparetuure in kelvin
        humidity = int(json_object['main']['humidity'])
        pressure = int(json_object['main']['pressure'])
        wind = int(json_object['wind']['speed'])

        #atlast just pass the variables
        condition = json_object['weather'][0]['main']
        desc = json_object['weather'][0]['description']
        Time = time.strftime('%A %B, %d %Y %H:%M:%S')
        
        client = MongoClient(host=IP,
                            port=27017, 
                            username='root', 
                            password='pass',
                            authSource="admin")
        db = client.users
        
        records = db.weather_collection
        records.insert_one({"Time":Time ,"email":email, "Country":city_name,"temperature": temperature, "humidity": humidity, "pressure": pressure, "wind": wind })
        
        return render_template('home.html',temperature=temperature,pressure=pressure,humidity=humidity,city_name=city_name,condition=condition,wind=wind,desc=desc, username=username)
    else:
        return render_template('home.html', username=username)

@app.route("/logout", methods=["POST", "GET"])
def logout():
    if "email" in session:
        session.pop("email", None)
        return render_template("signout.html")
    else:
        return render_template('index.html')




if __name__ == "__main__":
  app.run(debug=True, host='0.0.0.0', port=5000)
