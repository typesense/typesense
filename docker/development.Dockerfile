FROM dockcross/linux-x64

RUN apt-get update && apt-get install -y zlib1g-dev \
		libuv0.10-dev
