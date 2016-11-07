# EMR Bootstrap PySpark

This code should help to jump start PySpark with Anaconda on AWS.

## Getting Started
1. Upload scripts to your S3 bucket
2. `conda env create -f environment.yml`
3. Fill in all the required information e.g. aws access key, secret acess key etc. into the `config.yml.example` file and rename it to `config.yml`
4. Run it `python emr_loader.py`

## Requirements
- [Anaconda 3](https://www.continuum.io/downloads)
- [AWS Account](https://aws.amazon.com/)

## Copyright

See [LICENSE](LICENSE) for details.
Copyright (c) 2016 [Dat Tran](http://www.dat-tran.com/).