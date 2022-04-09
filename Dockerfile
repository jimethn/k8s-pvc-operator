FROM python
MAINTAINER Jonathan Lynch <jimethn@gmail.com>

COPY operator_pvc_manager /app
COPY requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

CMD ["/app/operator_pvc_manager.py"]
