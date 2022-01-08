#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <cstring>
using namespace std;

int main(int argc, char **argv) {
    if (argc < 2) {
        throw runtime_error("argc < 2");
    }
    const char *ips_file = argv[1];
    const char *dst_path = argv[2];
    const char *src_file = argv[3];

    auto start = chrono::steady_clock::now();
    vector<thread> ths;
    ifstream fin(ips_file);
    string ip;
    while (fin >> ip) {
        if (ip.empty()) continue;
        ths.emplace_back([&, ip] {
            auto start = chrono::steady_clock::now();
            char cmd[128];
            sprintf(cmd, "sshpass -p tongxing scp -r %s tongxing@%s:~/%s",
                    src_file, ip.c_str(), dst_path);

            FILE *pp = popen(cmd, "r");  // build pipe
            if (!pp) {
                printf("popen error, cmd: %s (len=%ld)\n", cmd, strlen(cmd));
                return;
            }
            pclose(pp);
            auto spend = chrono::duration_cast<chrono::milliseconds>(
                chrono::steady_clock::now() - start);
            printf("cmd: %s, spend time: %.2fs\n", cmd, spend.count() / 1000.0);
        });
    }

    for (auto &th : ths) th.join();

    auto spend = chrono::duration_cast<chrono::milliseconds>(
        chrono::steady_clock::now() - start);
    printf("spend time: %.2fs\n", spend.count() / 1000.0);

    return 0;
}
