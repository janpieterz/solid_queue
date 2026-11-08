source "https://rubygems.org"
git_source(:github) { |repo| "https://github.com/#{repo}.git" }

# Specify your gem's dependencies in solid_queue.gemspec.
gemspec

group :mssql, optional: true do
  gem "activerecord-sqlserver-adapter", ">= 7.1"
end
