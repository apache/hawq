git status|grep modified|awk '{print $2}'|xargs -I{} scp {}  root@vm1:/root/libyarn/{}
