import socket  # noqa: F401
import select

PORT=5000
# PORT=6379

class Server:
    def __init__(self, host="127.0.0.1", port=PORT):
        self.host = host
        self.port = port
        self.fd = None
        self.server_socket = None
        self.epoll = None
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

        while True:
            events = self.epoll.poll(1)

            for fd, event in events:
                sock = self.sockets[fd]
                if sock is self.server_socket:
                    self.accept_new_connection()
                else:
                    self.handle_client_events(fd, sock, event)

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
            
            command = data.decode(errors="ignore").strip()
            print(f"Client {fd} sent:  \"{command}\"")
            self.process_command(command, fd, sock)

    def process_command(self, command, fd, sock):
        if command == "":
            return
        if command.upper() == "PING":
            sock.send(b"+PONG\r\n")
        else:
            sock.send(b"-ERR unknown command\r\n")

    def handle_disconnect(self, fd, sock):
        print(f"Client {fd} disconnected!!")
        self.epoll.unregister(fd)
        sock.close()
        del self.sockets[fd]




def main():
    server = Server()
    server.start()

if __name__ == "__main__":
    main()
