import socket

def get_host_ip():
    try:
        with open('config/local_ip.txt', 'r') as f:
            ip = f.readline().strip()
            print("# get ip from local_ip.txt, ip:", ip)
            return ip
    except:
        pass
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    with open('config/local_ip.txt', 'w') as f:
        f.write(ip)
    print("# get ip from dial, ip:", ip)
    return ip

get_host_ip()