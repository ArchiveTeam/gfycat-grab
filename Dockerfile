FROM warcforceone/grab-base
COPY . /grab
RUN ln -s /usr/local/bin/wget-lua /grab/wget-lua
# RUN wget -O /grab/wget-lua http://xor.meo.ws/vUO6LyuhBlMOqGUjZ3sFQCqUcR83pl9N/wget-lua \
# && chmod +x /grab/wget-lua
