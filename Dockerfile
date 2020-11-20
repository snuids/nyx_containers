FROM python:3.7.3-slim
MAINTAINER lapin

RUN apt-get update
RUN apt-get install -y vim

COPY ./sources/requirements.txt /opt/sources/requirements.txt
RUN pip install -r /opt/sources/requirements.txt

COPY ./sources /opt/sources
RUN rm -d -r /opt/sources/logs
RUN mkdir  /opt/sources/logs
RUN rm -d -r /opt/sources/*.sh
#RUN rm -d -r /opt/sources/*.bat
#RUN rm -d -r /opt/sources/jaspergenerator
#RUN rm -d -r /opt/sources/reports

WORKDIR /opt/sources
#CMD ["python", "nyx_xlsimporter.py"]
#CMD ["python", "nyx_reportrunner.py"]
#CMD ["python", "biac_compute_instawork.py"]
#CMD ["python", "biac_import_instawork.py"]
#CMD ["python", "biac_import_spot_567.py"]
# CMD ["python", "biac_import_kizeo.py"]
# CMD ["python", "biac_import_feedback_comments.py"]
#CMD ["python", "biac_import_kpi105.py"]
#CMD ["python", "biac_month_kizeo.py"]
# CMD ["python", "biac_month_kizeo_2.py"]
#CMD ["python", "biac_import_bags.py"]
#CMD ["python","biac_import_bags.py"]
#CMD ["python","biac_mails.py"]
#CMD ["python","nyx_reportscheduler.py"]
#CMD ["python", "biac_mails.py"]
#CMD ["python", "biac_import_kpi305_501.py"]
#CMD ["python","nyx_formatconverter.py"]
#CMD ["python", "biac_availabilities_refactor.py"]
#CMD ["python", "biac_month_availability.py"]
#CMD ["python", "biac_lot7_computed.py"]
#CMD ["python", "biac_lot5_computed.py"]
#CMD ["python", "biac_import_kpi502.py"]
#CMD ["python", "lass_logis_dbfeeder.py"]
#CMD ["python", "par_import_coswin.py"]
#CMD ["python", "biac_import_maximo.py"]
#CMD ["python", "biac_maximo_calculated.py"]
#CMD ["python", "gtc_sites_data.py"]
#CMD ["python", "biac_lot2_monthly.py"]
#CMD ["python", "biac_import_kpi103.py"]
#CMD ["python", "biac_monthly_lot2.py"]
#CMD ["python", "biac_import_kpi102.py"]
#CMD ["python", "biac_import_waterloop.py"]
# CMD ["python", "biac_lot2_monthly.py"]
# CMD ["python", "biac_lot2_availabilities.py"]
#CMD ["python", "biac_import_kpi103.py"]
# CMD ["python", "biac_monthly_lot2.py"]
#CMD ["python", "biac_import_kpi102.py"]
#CMD ["python", "biac_import_feedback_comments.py"]
#CMD ["python", "biac_feedback_dispatcher.py"]
#CMD ["python", "gtc_sites_data.py"]
#CMD ["python", "gtc_import_onerp.py"]
CMD ["python","nyx_monitordocker.py"]
