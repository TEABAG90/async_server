import asyncio
import json
import redis
import socket


HOST = socket.gethostbyname(socket.gethostname())
PORT_1 = 8000
PORT_2 = 8001

db = redis.Redis(
    host='redis-12802.c285.us-west-2-2.ec2.cloud.redislabs.com',
    port=12802,
    password='UYp7OrivdXmUpkFH9Hcf1Kuqgkzu7NZI')

# В бесплатной подписке redis можно сделать только одну базу данных,
# поэтому в словаре DB только одна база, куда можно записывать логи
# с "dest" == 0
DB = {'0': db}

def report_handler(message):
    """Записывает сообщения в базу данных,
    в список с типом сообщения в качестве названия,
    например, "Warning" или "Problem on server"
    """
    if 'dest' not in message.keys() and 'data' not in message.keys():
        print('Message should contain dest or data fields!')
        return None
    dest = str(message['dest'])
    db = DB[dest]
    report = message['data']
    report = report.split(': ')
    report_type = report[0]
    report_msg = report[1]
    try:
        db.lpush(report_type, report_msg)
        return f'Message {report} successfully added!'
    except redis.exceptions.ConnectionError:
        print('Connection with server unavailable!')
    

async def conn_handler(reader,writer):
    while True:
        data = await reader.read(100)
        try:
            message = json.loads(data)
            res = report_handler(message)
            if res:
                print(res)
        except json.decoder.JSONDecodeError: 
            print ('Unknown data format')

async def async_server():
    server_1 = await asyncio.start_server(conn_handler, HOST, PORT_1)
    server_2 = await asyncio.start_server(conn_handler, HOST, PORT_2)
    addr_1 = ', '.join(str(sock.getsockname()) for sock in server_1.sockets)
    addr_2 = ', '.join(str(sock.getsockname()) for sock in server_2.sockets)
    print(f'Serving on {addr_1}, {addr_2}')
    await server_1.serve_forever()
    await server_2.serve_forever()


asyncio.run(async_server())
