#!bin/sh

mkdir -p lambda_layers/python
cd lambda_layers/python

pip3 install nltk -t ./
python -W ignore -m nltk.downloader stopwords -d ./nltk_data
python -W ignore -m nltk.downloader vader_lexicon -d ./nltk_data

cd ..
zip -r ../python3.8-modules.zip python

cd ..
rm -r lambda_layers


