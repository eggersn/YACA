import socket 

def get_host_ip():
    """Return the local IP address"""
    try:
        ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ss.connect(('8.8.8.8', 8070))
        ip = ss.getsockname()[0]
    finally:
        ss.close()
    return ip