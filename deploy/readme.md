# 构建基础镜像
进入 build_docker_image 目录，构建基础镜像用于离线部署

## 联网构建镜像
./build_docker_image.sh

## 保存构建后的基础镜像到本机
./save_docker_image.sh

## 从本机加载基础镜像到Docker
./load_docker_image.sh


# 安装服务
./install.sh

# 卸载服务
./remove.sh

# 调试模式

安装
./installdebug.sh

进入容器后，执行启动脚本即可运行
./start.sh

卸载
./removedebug.sh
