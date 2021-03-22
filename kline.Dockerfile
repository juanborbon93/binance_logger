FROM public.ecr.aws/lambda/python:3.8
RUN /var/lang/bin/python3.8 -m pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app/ /var/task/

CMD ["crypto_logger.log_klines"]