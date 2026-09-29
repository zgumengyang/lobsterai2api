// tunnel-client：本机超小转发器
// 监听 127.0.0.1:18367（浏览器登录回调落点）→ 转发到服务器面板隧道网关 8369
// 服务器侧：8369 → 127.0.0.1:18367（助手）
package main

import (
	"flag"
	"io"
	"log"
	"net"
	"time"
)

var (
	listen = flag.String("listen", "127.0.0.1:18367", "本机监听地址")
	server = flag.String("server", "124.221.39.166:8369", "服务器隧道网关地址")
)

func main() {
	flag.Parse()
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("tunnel-client on %s -> %s（登录回调转发中，保持本窗口运行）", *listen, *server)
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			up, err := net.DialTimeout("tcp", *server, 8*time.Second)
			if err != nil {
				log.Printf("dial server: %v", err)
				return
			}
			defer up.Close()
			go func() {
				_, _ = io.Copy(up, c)
				_ = up.Close()
			}()
			_, _ = io.Copy(c, up)
		}(c)
	}
}
