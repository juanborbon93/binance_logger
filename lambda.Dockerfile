FROM public.ecr.aws/lambda/python:3.7
RUN /var/lang/bin/python3.7 -m pip install --upgrade pip

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app/ .

CMD ["index.handler"]