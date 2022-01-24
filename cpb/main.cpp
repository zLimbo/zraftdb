#include <sys/unistd.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <error.h>
#include <fstream>
#include <cstdio>
#include <string>
#include <unordered_map>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <mutex>

#include "hlib/json.hpp"
#include "hlib/zfish.hpp"

namespace nh = nlohmann;
namespace chrono = std::chrono;

const int kMaxEventNum = 1024;
const int kBufSize = 1024;

nh::json initConfig(const std::string& file) {
    std::ifstream fin(file);
    return nh::json::parse(fin);
}

const std::string getLocalIp(const std::string& file) {
    std::ifstream fin(file);
    std::string local_ip;
    fin >> local_ip;
    return local_ip;
}

void addNioFd(int epoll_fd, int fd) {
    int flag = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, flag | O_NONBLOCK);
    struct epoll_event event;
    event.data.fd = fd;
    event.events = EPOLLIN | EPOLLET;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event);
}

int main() {
    nh::json config = initConfig("config/config.json");
    const std::string local_ip = getLocalIp("config/local_ip.txt");
    int port = config["PortBase"].get<int>() + 1;
    printf("config: %s\n", config.dump().c_str());
    printf("addr: %s:%d\n", local_ip.c_str(), port);

    std::vector<std::string> peer_ips;
    for (auto& v : config["PeerIps"]) {
        peer_ips.push_back(v.get<std::string>());
    }

    struct sockaddr_in addr;
    bzero(&addr, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(local_ip.c_str());
    addr.sin_port = htons(port);

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(listen_fd >= 0);
    int ret = bind(listen_fd, (sockaddr*)&addr, sizeof(addr));
    assert(ret >= 0);
    ret = listen(listen_fd, 20);
    assert(ret >= 0);

    int epoll_fd = epoll_create(20);
    assert(epoll_fd >= 0);
    addNioFd(epoll_fd, listen_fd);

    epoll_event events[kMaxEventNum];

    std::unordered_map<std::string, int> node;
    std::mutex mtx;

    zfish::ThreadPool pool(20);
    pool.put([&] {
        while (true) {
            printf("epoll wait ...\n");
            int num = epoll_wait(epoll_fd, events, kMaxEventNum, -1);
            assert(num >= 0);
            for (int i = 0; i < num; ++i) {
                if (events[i].data.fd == listen_fd) {
                    while (true) {
                        sockaddr_in addr;
                        socklen_t len = sizeof(addr);
                        int conn_fd = accept(listen_fd, (sockaddr*)&addr, &len);
                        if (conn_fd < 0) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            }
                            throw std::runtime_error("accept error");
                        }
                        addNioFd(epoll_fd, conn_fd);
                    }
                } else {
                    int conn_fd = events[i].data.fd;
                    char buf[kBufSize];
                    while (true) {
                        int nRead = read(conn_fd, buf, kBufSize - 1);
                        if (nRead < 0) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            }
                            throw std::runtime_error("read error");
                        } else if (nRead == 0) {
                            break;
                        }
                        buf[nRead] = '\0';
                    }
                    std::string ip{buf};
                    {
                        std::lock_guard<std::mutex>{mtx};
                        if (node[ip]) continue;
                        node[ip] = conn_fd;
                    }
                    printf("connect %s success\n", ip.c_str());
                }
            }
        }
    });

    // zfish::BlockingQueue bq;

    for (std::string& ip: peer_ips) {

    
        pool.put([&, ip] {;
            sockaddr_in addr;
            printf("try connect %s\n", ip.c_str());
            bzero(&addr, sizeof(addr));
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = inet_addr(ip.c_str());
            addr.sin_port = htons(port);
            int conn_fd = socket(AF_INET, SOCK_STREAM, 0);
            assert(conn_fd >= 0);
            int ret = connect(conn_fd, (sockaddr*)&addr, sizeof(addr));
            if (ret < 0) return;
            ret = send(conn_fd, local_ip.c_str(), local_ip.size(), 0);
            if (ret < 0 || ret != local_ip.size()) return;
            char buf[1];
            ret = recv(conn_fd, buf, 1, 0);
            if (ret < 0 || ret != 1) return;
            {
                std::lock_guard<std::mutex>{mtx};
                if (node[ip]) return;
                node[ip] = conn_fd;
            }
            addNioFd(epoll_fd, conn_fd);
            printf("connect %s success\n", ip.c_str());
        });
    }

    return 0;
}

