# 来源与对齐记录

- 2026-10-28 从 github.com/viccom/tsink master HEAD 恢复（tarball 方式，替
  代两行 stub），当时 HEAD 7f16d59（N8）
- 2026-10-29 与远程对齐为完整 git 克隆：HEAD 50ba2a5（第五轮 P3 水位粗判
  Cut2，2026-09-03 自 Windows worktree 补推）。remote = origin
  https://github.com/viccom/tsink.git
- git 走代理要点：gnutls 经 HTTP/2 CONNECT 握手失败，已设
  `git config --global http.version HTTP/1.1` 解决，pull/push 正常
- tsink 侧第五轮（2bf1687/e10de8a/50ba2a5）为纯引擎侧改进（evict 谓词水位
  化），无 API 变化，rs-iot 接入面零改动
