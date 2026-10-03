FROM ruby:3.4

WORKDIR /app

RUN apt-get update -qq && \
    apt-get install --no-install-recommends -y build-essential chromium chromium-driver git libpq-dev libyaml-dev pkg-config postgresql-client && \
    rm -rf /var/lib/apt/lists /var/cache/apt/archives

ENV BUNDLE_PATH="/usr/local/bundle" \
    CAPYBARA_SERVER_HOST="0.0.0.0" \
    CHROME_BIN="/usr/bin/chromium"

COPY Gemfile Gemfile.lock ./
RUN bundle install

COPY . .
