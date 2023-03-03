# bash ci_build.sh typesense-server|typesense-test version
docker run -it --rm -v $HOME/docker_bazel_cache:$HOME/docker_bazel_cache -v $PWD:/src \
--workdir /src typesense/bazel_dev:03032023 bazel --output_user_root=$HOME/docker_bazel_cache/cache build \
--jobs=6 --action_env=LD_LIBRARY_PATH="/usr/local/gcc-10.3.0/lib64" --define=TYPESENSE_VERSION=\"$2\" //:$1
