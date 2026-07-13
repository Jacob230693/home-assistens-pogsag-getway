FROM ghcr.io/home-assistant/base:latest

ARG BUILD_VERSION
ARG BUILD_ARCH

LABEL \
  io.hass.version="${BUILD_VERSION}" \
  io.hass.type="app" \
  io.hass.arch="${BUILD_ARCH}"

RUN apk add --no-cache \
      bash \
      build-base \
      cmake \
      git \
      pkgconf \
      libusb-dev \
      libusb \
      python3 \
      py3-paho-mqtt \
      ca-certificates \
  && git clone --depth=1 https://github.com/rtlsdrblog/rtl-sdr-blog.git /tmp/rtl-sdr \
  && cmake -S /tmp/rtl-sdr -B /tmp/rtl-sdr/build \
      -DCMAKE_INSTALL_PREFIX=/usr \
      -DDETACH_KERNEL_DRIVER=ON \
      -DINSTALL_UDEV_RULES=OFF \
  && cmake --build /tmp/rtl-sdr/build --parallel 2 \
  && cmake --install /tmp/rtl-sdr/build \
  && git clone --depth=1 https://github.com/EliasOenal/multimon-ng.git /tmp/multimon-ng \
  && cmake -S /tmp/multimon-ng -B /tmp/multimon-ng/build \
      -DCMAKE_INSTALL_PREFIX=/usr \
  && cmake --build /tmp/multimon-ng/build --parallel 2 \
  && cmake --install /tmp/multimon-ng/build \
  && rm -rf /tmp/rtl-sdr /tmp/multimon-ng

COPY run.sh /run.sh
COPY parser.py /parser.py
RUN chmod a+x /run.sh /parser.py

CMD ["/run.sh"]
