import socket  # noqa: F401
import select
import time

PORT=5000

class Server:
    def __init__(self, host="127.0.0.1", port=PORT):
        self.host = host
        self.port = port
        self.fd = None
        self.server_socket = None
        self.epoll = None
        self.db = {}
        self.expiry = {}
        self.sockets = {}

    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(128)
        self.server_socket.setblocking(False)

        print (f"Server running and listening on {self.host}:{self.port}")

        self.epoll = select.epoll()
        self.fd = self.server_socket.fileno()
        self.epoll.register(self.fd, select.EPOLLIN | select.EPOLLOUT)
        self.sockets[self.fd] = self.server_socket
        
        try:
            while True:
                events = self.epoll.poll(1)
                for fd, event in events:
                    sock = self.sockets[fd]
                    if sock is self.server_socket:
                        self.accept_new_connection()
                    else:
                        self.handle_client_events(fd, sock, event)
        except KeyboardInterrupt:
            self.shutdown()

    def shutdown(self):
        print("\nshutting the server down...")
        for fd, sock in list(self.sockets.items()):
            try:
                self.epoll.unregister(fd)
            except Exception:
                pass
            
            try:
                sock.close()
            except:
                pass
            
        try:
            self.epoll.close()
        except Exception:
            pass

        print("Server stopped!!!")

    def accept_new_connection(self):
        try:
            client_socket, addr = self.server_socket.accept()
        except BlockingIOError:
            return

        client_socket.setblocking(False)
        client_fd = client_socket.fileno()
        self.epoll.register(client_fd, select.EPOLLIN | select.EPOLLOUT)
        self.sockets[client_fd] = client_socket
        client_socket.send(b"Welcome to my miniRedis server\r\n")
        print(f"Client connected: {addr}")

    def handle_client_events(self, fd, sock, event):
        if event & select.EPOLLIN:
            try:
                data = sock.recv(1024)
            except BlockingIOError:
                return 
            
            if not data:
                self.handle_disconnect(fd, sock)
                return
            cmd, args = self.parse_command_line(data)
            # command = data.decode(errors="ignore")
            self.process_command(cmd, args, fd, sock)

    def process_command(self, cmd, args, fd, sock):
        print(f"Client {fd} sent:  \"{cmd} {args[0:]}\"")
        if cmd == "":
            return
        
        if cmd == "PING":
            sock.send(b"+PONG\r\n")
        elif cmd == "ECHO":
            self.echo(args, sock)
        elif cmd == "SET":
            self.set(args, sock)
        elif cmd == "GET":
            self.get(args, sock)
        else:
            sock.send(b"-ERR unknown command\r\n")

    def get(self, args, sock):
        if len(args) != 1:
            sock.send(b"-ERR wrong number of arguments for 'set' command\r\n")
            return
        
        key = args[0]
        
        if key in self.expiry:
            if (time.time() >= self.expiry[key]):
                del self.db[key]
                del self.expiry[key]

        if key in self.db:
            value = self.db[key]
            sock.send(f"${len(value)}\r\n{value}\r\n".encode())
        else:
            sock.send(b"$-1\r\n")

    def set(self, args, sock):
        if len(args) < 2:
            sock.send(b"-ERR wrong number of arguments for 'set' command\r\n")
            return
        
        key, value = args[0], args[1]
        time_s = None
        if len(args) > 2:
            i = 2
            while i < len(args):
                opt = args[i].upper()
                
                if opt == "EX":
                    if i + 1 >= len(args):
                        sock.send(b"-ERR syntax error\r\n")
                        return
                    try:
                        time_s = int(args[i+1])    # seconds
                    except ValueError:
                        sock.send(b"-ERR value is not an integer or out of range\r\n")
                        return
                    i += 2

                elif opt == "PX":
                    if i + 1 >= len(args):
                        sock.send(b"-ERR syntax error\r\n")
                        return
                    try:
                        ttl_ms = int(args[i+1])
                        time_s = ttl_ms / 1000.0   # convert to seconds
                    except ValueError:
                        sock.send(b"-ERR value is not an integer or out of range\r\n")
                        return
                    i += 2

                else:
                    sock.send(b"-ERR syntax error\r\n")
                    return
        self.db[key] = value
        if time_s is not None:
            self.expiry[key] = time.time() + time_s
        elif key in self.expiry:
            del self.expiry[key]
        sock.send(b"+OK\r\n")
        

    def echo(self, args, sock):
        if not args:
            sock.send(b"-ERR wrong number of arguments for 'echo' command\r\n")
            return
        
        msg = args[0]
        sock.send(f"${len(msg)}\r\n{msg}\r\n".encode())

    def handle_disconnect(self, fd, sock):
        print(f"Client {fd} disconnected!!")
        self.epoll.unregister(fd)
        sock.close()
        del self.sockets[fd]

    def	parse_command_line(self, raw: bytes):
        command = raw.decode()        
        if not command.startswith("*"):
            line = command.strip().split()
            if not line:
                return "", []
            return line[0].upper(), line[1:]
        
        lines = command.split("\r\n")
        argc = int(lines[0][1:])
        args = []
        i = 1
        while i < len(lines) and len(args) < argc:
            if (lines[i].startswith("$")):
                args.append(lines[i + 1])
                i += 2
            else:
                i += 1
        if not args:
            return "", []
        return args[0].upper(), args[1:]
            
