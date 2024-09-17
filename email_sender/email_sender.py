import os
import sys
import smtplib
import threading
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email import encoders
from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
from PIL import Image
import requests
import time
import docker

def get_mongo_ip():
    # Connect to Docker
    client = docker.from_env()

    # Get the MongoDB container
    container = client.containers.get('open_weather_app_db_1')

    # Get the IP address of the MongoDB container
    mongo_networks = container.attrs['NetworkSettings']['Networks']
    mongo_network_name = next(iter(mongo_networks))
    mongo_ip = mongo_networks[mongo_network_name]['IPAddress']

    return mongo_ip

#to know the ip address of the mongo container
#docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' kafka_test_db_1
# Connect to the MongoDB database
def get_email_addresses():
    client = MongoClient(host=get_mongo_ip(),
                            port=27017, 
                            username='root', 
                            password='pass',
                            authSource="admin")
    db = client.users
    records = db.register.find({})
    email_addresses = {}
    for record in records:
        email_addresses[record['name']] = record['email']
    return email_addresses

def get_country_names():
    client = MongoClient(host=get_mongo_ip(),
                            port=27017, 
                            username='root', 
                            password='pass',
                            authSource="admin")
    db = client.users
    records = db.register.find({})
    country_names = {}
    for record in records:
        country_names[record['name']] = record['Country']
    return country_names
    
def send_email(topic, recipient, condition, humidity):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        group_id='my-group'
    )
    for message in consumer:
        value = int(message.value.decode())
        if value > 30:
            # Set up email details
            email = os.getenv('EMAIL')
            password = os.getenv('PASSWORD')
            subject = f'{topic} temperature Alert!!'
            body = f"The current temperature in {topic} is {value}C. It's seems very hot Today"

            photo_path2 = '/home/emna/Bureau/kafka_test/OpenWeather.jpg'
            photo_path = photo_path2
            
            photo_path = '/home/emna/Bureau/with_docker/static/img/Heat_Alert.jpg'
            
            # Create message container
            msg = MIMEMultipart()

            # Read the photo as binary data and attach it to the email
            with open(photo_path, 'rb') as f:
                img_data = f.read()
                image_part = MIMEImage(img_data, name='Current Weather')
                image_part.add_header('Content-ID', '<image1>')
                msg.attach(image_part)

            # Read the photo as binary data and attach it to the email
            with open(photo_path2, 'rb') as f:
                img_data = f.read()
                image_part = MIMEImage(img_data, name='OpenWeather')
                image_part.add_header('Content-ID', '<image2>')
                msg.attach(image_part)

            html = f"""\
            <html>
            <body>
                <p style="font-size:20px; font-family: Montserrat, sans-serif;">{body}</p>
                <table>
                    <tr>
                        <td><img src="cid:image1" width="300" height="300"></td>
                        <td><img src="cid:image2" width="500" height="300"></td>
                    </tr>
                </table>
            </body>
            </html>
            """

            # Attach the HTML content to the email as a MIMEText object
            body = MIMEText(html, 'html')
            msg.attach(body)
            # Set up other message fields
            msg['From'] = email
            msg['To'] = email
            msg['Subject'] = subject

            # Send the message via SMTP server
            s = smtplib.SMTP('smtp.gmail.com', 587)
            s.starttls()
            s.login(email, password)
            s.sendmail(email, recipient, msg.as_string())
            s.quit()

            consumer.close()
        elif humidity > 60:
            # Set up email details
            email = 'lilia.krichen@gmail.com'
            password = 'bpqeyalsmbsitbpk'
            subject = f'{topic} temperature Alert!!'
            body = f"The current temperature in {topic} is {value}C. The humidity is high around {humidity}. Be careful and stay hydrated !"

            photo_path2 = '/home/emna/Bureau/kafka_test/OpenWeather.jpg'
            photo_path = photo_path2
            
            photo_path = '/home/emna/Bureau/with_docker/static/img/humidity.png'
            
            # Create message container
            msg = MIMEMultipart()

            # Read the photo as binary data and attach it to the email
            with open(photo_path, 'rb') as f:
                img_data = f.read()
                image_part = MIMEImage(img_data, name='Current Weather')
                image_part.add_header('Content-ID', '<image1>')
                msg.attach(image_part)

            # Read the photo as binary data and attach it to the email
            with open(photo_path2, 'rb') as f:
                img_data = f.read()
                image_part = MIMEImage(img_data, name='OpenWeather')
                image_part.add_header('Content-ID', '<image2>')
                msg.attach(image_part)

            html = f"""\
            <html>
            <body>
                <p style="font-size:20px; font-family: Montserrat, sans-serif;">{body}</p>
                <table>
                    <tr>
                        <td><img src="cid:image1" width="300" height="300"></td>
                        <td><img src="cid:image2" width="500" height="300"></td>
                    </tr>
                </table>
            </body>
            </html>
            """

            # Attach the HTML content to the email as a MIMEText object
            body = MIMEText(html, 'html')
            msg.attach(body)
            # Set up other message fields
            msg['From'] = email
            msg['To'] = email
            msg['Subject'] = subject

            # Send the message via SMTP server
            s = smtplib.SMTP('smtp.gmail.com', 587)
            s.starttls()
            s.login(email, password)
            s.sendmail(email, recipient, msg.as_string())
            s.quit()

            consumer.close()	
        else:
            # Set up email details
            email = os.getenv('EMAIL')
            password = os.getenv('PASSWORD')
            subject = f'{topic} temperature'
            body = f'The current temperature in {topic} is {value}C.'
            photo_path2 = '/home/emna/Bureau/kafka_test/OpenWeather.jpg'
            photo_path = photo_path2
            
            if condition == 'Clear':
                photo_path = '/home/emna/Bureau/with_docker/static/img/clear-sky.png'
            elif condition == 'Rain':
                photo_path = '/home/emna/Bureau/with_docker/static/img/rain_light.png'
            elif condition == 'Clouds':
                photo_path = '/home/emna/Bureau/with_docker/static/img/clouds.png'
            elif condition == 'Snow':
                photo_path = '/home/emna/Bureau/with_docker/static/img/snow.png'
            elif condition == 'Thunderstorm':
                photo_path = '/home/emna/Bureau/with_docker/static/img/thunder_storm.png'
            elif condition == 'Haze':
                photo_path = '/home/emna/Bureau/with_docker/static/img/haze.png'
            
            # Create message container
            msg = MIMEMultipart()

            # Read the photo as binary data and attach it to the email
            with open(photo_path, 'rb') as f:
                img_data = f.read()
                image_part = MIMEImage(img_data, name='Current Weather')
                image_part.add_header('Content-ID', '<image1>')
                msg.attach(image_part)

            # Read the photo as binary data and attach it to the email
            with open(photo_path2, 'rb') as f:
                img_data = f.read()
                image_part = MIMEImage(img_data, name='OpenWeather')
                image_part.add_header('Content-ID', '<image2>')
                msg.attach(image_part)

            html = f"""\
            <html>
            <body>
                <p style="font-size:20px; font-family: Montserrat, sans-serif;">{body}</p>
                <table>
                    <tr>
                        <td><img src="cid:image1" width="300" height="300"></td>
                        <td><img src="cid:image2" width="500" height="300"></td>
                    </tr>
                </table>
            </body>
            </html>
            """

            # Attach the HTML content to the email as a MIMEText object
            body = MIMEText(html, 'html')
            msg.attach(body)
            # Set up other message fields
            msg['From'] = email
            msg['To'] = email
            msg['Subject'] = subject

            # Send the message via SMTP server
            s = smtplib.SMTP('smtp.gmail.com', 587)
            s.starttls()
            s.login(email, password)
            s.sendmail(email, recipient, msg.as_string())
            s.quit()

            consumer.close()

while True:
    # Create a mutex object
    file_mutex = threading.Lock()
     
    print(get_email_addresses())

    # Set up the Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092']
    )

    print(get_country_names())

    # Get the country names from the MongoDB database
    country_names = get_country_names()

    # Convert the dictionary to a list of tuples
    country_list = list(country_names.items())


    # Keep track of topics that have already been sent
    sent_topics = set()

    # Send data to the Kafka topics
    time.sleep(5)
    for i in range(len(country_list)):
        topic_name = country_list[i][1]
        #topic_name = 'London'
        r = requests.get('https://api.openweathermap.org/data/2.5/weather?q='+topic_name+'&appid=94d3b2c21850c2a5da8ad81d24b4480e')
        #read the json object
        json_object = r.json()

        #take some attributes like temperature,humidity,pressure of this 
        temperature = int(json_object['main']['temp']-273.15) #this temparetuure in kelvin
        condition = json_object['weather'][0]['main']
        humidity = int(json_object['main']['humidity'])

        temperature_str = str(temperature).encode('utf-8')
        producer.send(topic_name, temperature_str)

    # Create a thread for each topic with the appropriate recipient email address
    threads = []
    # Get the email addresses from the MongoDB database
    email_addresses = get_email_addresses()

    # Convert the dictionary to a list of tuples
    email_list = list(email_addresses.items())


    # Get the list of countries as key-value pairs
    country_list = list(country_names.items())

    # Create a dictionary of topic names and recipient email addresses
    topic_recipients = {}
    for i in range(min(len(country_list), len(email_list))):
        country = country_list[i][1]
        email_address = email_list[i][1]
        if country in topic_recipients:
            topic_recipients[country].append(email_address)
        else:
            topic_recipients[country] = [email_address]
    
    print(topic_recipients)
    
    for topic, recipient in topic_recipients.items():
        for email in recipient:
            thread = threading.Thread(target=send_email, args=(topic, recipient, condition, humidity))
            thread.start()
            threads.append(thread)

    # Wait for threads to finish
    for thread in threads:
        try:
            thread.join()
        except KeyboardInterrupt:
            print("\nArret du programme ...\n")
            sys.exit()

