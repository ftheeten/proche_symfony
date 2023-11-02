#https://github.com/docker-library/php/blob/7f89b1440e0122cc0edaf4ee0dbd8e1d7510df73/8.2/alpine3.18/fpm/Dockerfile
FROM php:8.2.11-fpm-alpine3.18

RUN apk update

RUN apk add --no-cache icu
RUN apk add --no-cache icu-dev
RUN apk add --no-cache icu-libs

RUN docker-php-ext-configure intl
RUN docker-php-ext-install intl

RUN apk add --no-cache php82-intl

RUN apk add --no-cache php82-mbstring
EXPOSE 9000


CMD ["php-fpm"]