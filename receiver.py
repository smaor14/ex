import pika
import json
import xml
import csv
import sqlite3
from sqlite3 import Error

#open connection with DB
def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return None
#execute selects and creat files
def selectExecuting(conn , select , formating , fields , fileName):    
    cur = conn.cursor()
    cur.execute(select)
    rows = cur.fetchall()
    if (formating == "csv"):
        fileName = fileName + '.csv'
        with open (fileName , 'w' , encoding="utf-8") as file:
            f = csv.writer(file)
            f.writerow(fields)
            for row in rows:
                f.writerow(row)
    elif (formating == "json"):
        fileName = fileName + '.json'
        with open(fileName , 'w') as file:
            file.write(json.dumps(rows))     
        file.close()
#    elif (formating == "xml"):
#        print("[X] rows %r" % rows)
#        with open('file.xml' , 'w') as file:
#            xml = ''.join(row[0] for row in rows)
#            file.write(rows)     
#        file.close()
    else:
        if (formating == "table"):
            tableName = fileName
            drop = "drop table if exists " + tableName
            cur.execute(drop)
            conn.commit()
            create = "create table " + tableName + " as " + select  
            cur.execute(create)
            conn.commit()
        else:
            print(' [*] please choose another format and try again')
            


#build select statment and parameters
def execSql(path,formating):
    conn = create_connection(path)
    selectSongDetails = "select t.Name as songName, a1.Name as singerName   , m.Name as typeName \
                from tracks t \
                join albums a2 on t.AlbumId = a2.AlbumId \
                join media_types m on t.MediaTypeId = m.MediaTypeId \
                join artists a1 on a1.ArtistId = a2.ArtistId"
    songFields = (['songName' , 'singerName' , 'typeName'])
    selectCustomerDetails =  "select (c.FirstName || ' ' || c.LastName) as customerName ,  \
                           c.Phone as phone , \
                           (c.Address || ' ' || c.City || ' ' || c.State || ' ' || c.Country || ' ' || c.PostalCode) as customerAdrs , \
                           count(invoiceId) as count\
                    from customers c \
                    join invoices i on i.CustomerId = c.CustomerId \
                    group by i.CustomerId"
    customerFields = (['customerName' , 'phone' , 'customerAdrs', 'count'])
    selectDisksByCountry = "select c.Country as country, count(a.AlbumId) as countOfDisks \
                            from customers c \
                            join invoices i on i.CustomerId = c.CustomerId \
                            join invoice_items i2 on i2.InvoiceId = i.InvoiceId \
                            join tracks t on t.TrackId = i2.TrackId \
                            join albums a on a.AlbumId = t.AlbumId \
                            group by c.country"
    disksFields = (['country' , 'countOfDisks'])
    selectTracksByCountry = "select c.Country as country , count(i2.TrackId) as countOfSongs \
                             from customers c \
                             join invoices i on i.CustomerId = c.CustomerId \
                             join invoice_items i2 on i2.InvoiceId = i.InvoiceId \
                             group by c.Country"
    songsByCountryFields = (['country' , 'countOfSongs'])

    selectExecuting(conn , selectSongDetails , formating , songFields , 'songDetails')
    selectExecuting(conn , selectCustomerDetails , formating , customerFields , 'customerDetails')
    selectExecuting(conn , selectDisksByCountry , formating ,disksFields , 'disksByCountry' )
    selectExecuting(conn , selectTracksByCountry , formating , songsByCountryFields , 'songsByCountry')
    
#connect to RabbitMQ and open channel
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

#queue declaretion
channel.queue_declare(queue='message')

#get message details and call select functions
def callback(ch, method, properties, body):
    data = json.loads(body)
    path = data['path']
    formating = data['format']
    print("[X] path %r" % path)
    print("[X] format %r" % formating)
    execSql(path,formating)
    connection.close()
channel.basic_consume(callback,
                      queue='message',
                      no_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
#start consuming between sender and receiver
channel.start_consuming()

