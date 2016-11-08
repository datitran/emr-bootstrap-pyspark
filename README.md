# EMR Bootstrap PySpark with Anaconda

This code should help to jump start PySpark with Anaconda on AWS.

## Getting Started
1. `conda env create -f environment.yml`
2. Fill in all the required information e.g. aws access key, secret acess key etc. into the `config.yml.example` file and rename it to `config.yml`
3. Run it `python emr_loader.py`

## Requirements
- [Anaconda 3](https://www.continuum.io/downloads)
- [AWS Account](https://aws.amazon.com/)

## Copyright

See [LICENSE](LICENSE) for details.
Copyright (c) 2016 [Dat Tran](http://www.dat-tran.com/).