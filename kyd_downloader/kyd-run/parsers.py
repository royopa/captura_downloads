import logging
import os
import re
import tempfile
from datetime import datetime, timezone

import google.cloud.storage as storage
import pandas as pd
from google.cloud.storage.blob import Blob

from kyd.parsers import unzip_file_to
from kyd.parsers.anbima import AnbimaDebentures, AnbimaTPF, AnbimaVnaTPF
from kyd.parsers.b3 import (
    BVBG028,
    BVBG086,
    BVBG087,
    CDIIDI,
    COTAHIST,
    StockIndexInfo,
)

logging.basicConfig(level=logging.INFO)


class Processor:
    def format_refdate(self, refdate: str):
        if self.name == 'cotahist':
            return refdate[:4]
        else:
            m = re.search(r'\d{4}-\d\d-\d\d', refdate)
            return m.group() if m else None

    def process(self, fobj, fname, log):
        raise NotImplemented()


class BVBG028Processor(Processor):
    name = 'cadinstr'

    @property
    def layer1_name(self):
        return 'BVBG028'

    def process(self, fobj, fname, log):
        temp = tempfile.gettempdir()
        tempfname = unzip_file_to(fobj, temp)
        x = BVBG028(tempfname)

        instrs = {}
        for instr in x.data:
            typo = instr['instrument_type']
            try:
                instrs[typo].append(instr)
            except:
                instrs[typo] = [instr]

        return {k: pd.DataFrame(instrs[k]) for k in instrs.keys()}


class BVBG086Processor(Processor):
    name = 'pricereport'

    @property
    def layer1_name(self):
        return 'BVBG086'

    def process(self, fobj, fname, log):
        temp = tempfile.gettempdir()
        tempfname = unzip_file_to(fobj, temp)
        x = BVBG086(tempfname)
        return {'data': pd.DataFrame(x.data)}


class BVBG087Processor(Processor):
    name = 'indexreport'

    @property
    def layer1_name(self):
        return 'BVBG087'

    def process(self, fobj, fname, log):
        temp = tempfile.gettempdir()
        tempfname = unzip_file_to(fobj, temp)
        x = BVBG087(tempfname)

        instrs = {}
        for instr in x.data:
            typo = instr['index_type']
            try:
                instrs[typo].append(instr)
            except:
                instrs[typo] = [instr]

        return {k: pd.DataFrame(instrs[k]) for k in instrs.keys()}


class AnbimaTitpubProcessor(Processor):
    name = 'titpub_anbima'

    @property
    def layer1_name(self):
        return 'AnbimaTitpub'

    def process(self, fobj, fname, log):
        x = AnbimaTPF(fname)
        df = pd.DataFrame(x.data)
        df['ask_yield'] = pd.to_numeric(
            df['ask_yield'], errors='coerce'
        ).astype(float)
        df['bid_yield'] = pd.to_numeric(
            df['bid_yield'], errors='coerce'
        ).astype(float)
        df['ref_yield'] = pd.to_numeric(
            df['ref_yield'], errors='coerce'
        ).astype(float)
        df['price'] = pd.to_numeric(df['price'], errors='coerce').astype(float)
        x = ~df['ref_yield'].isna()
        if any(x):
            df = df[x].copy()
        else:
            raise Exception('Invalid data')
        df['cod_selic'] = df['cod_selic'].astype(int)
        return {'data': df}


class AnbimaVnaTitpubProcessor(Processor):
    name = 'vna_anbima'

    @property
    def layer1_name(self):
        return 'AnbimaVnaTitpub'

    def process(self, fobj, fname, log):
        x = AnbimaVnaTPF(fname)
        if len(x.data) == 0:
            raise Exception('Invalid data')
        df = pd.DataFrame(x.data)
        return {'data': df}


class DebenturesProcessor(Processor):
    name = 'deb_anbima'

    @property
    def layer1_name(self):
        return 'AnbimaDebentures'

    def process(self, fobj, fname, log):
        x = AnbimaDebentures(fname)
        if len(x.data) == 0:
            raise Exception('Invalid data')
        df = pd.DataFrame(x.data)
        df['refdate'] = log['refdate']
        df['ask_yield'] = pd.to_numeric(
            df['ask_yield'], errors='coerce'
        ).astype(float)
        df['bid_yield'] = pd.to_numeric(
            df['bid_yield'], errors='coerce'
        ).astype(float)
        df['ref_yield'] = pd.to_numeric(
            df['ref_yield'], errors='coerce'
        ).astype(float)
        df['price'] = pd.to_numeric(df['price'], errors='coerce').astype(float)
        df['perc_price_par'] = pd.to_numeric(
            df['perc_price_par'], errors='coerce'
        ).astype(float)
        df['duration'] = pd.to_numeric(df['duration'], errors='coerce').astype(
            float
        )
        df['perc_reune'] = pd.to_numeric(
            df['perc_reune'], errors='coerce'
        ).astype(float)
        return {'data': df}


class COTAHISTProcessor(Processor):
    name = 'cotahist'

    @property
    def layer1_name(self):
        return 'COTAHIST'

    def process(self, fobj, fname, log):
        temp = tempfile.gettempdir()
        tempfname = unzip_file_to(fobj, temp)
        x = COTAHIST(tempfname)
        return {'data': pd.DataFrame(x.data)}


class StockIndexInfoProcessor(Processor):
    name = 'b3-stock-index-info'

    @property
    def layer1_name(self):
        return 'B3StockIndexInfo'

    def process(self, fobj, fname, log):
        x = StockIndexInfo(fname)
        if len(x.data) == 0:
            raise Exception('Invalid data')
        return {'data': x.data}


class CDIProcessor(Processor):
    name = 'cdi'

    @property
    def layer1_name(self):
        return 'CDI'

    def process(self, fobj, fname, log):
        x = CDIIDI(fname)
        if len(x.data) == 0:
            raise Exception('Invalid data')
        return {'data': pd.DataFrame(x.data)}


class ProcessorFactory:
    PROCESSORS = {
        BVBG028Processor.name: BVBG028Processor(),
        BVBG086Processor.name: BVBG086Processor(),
        COTAHISTProcessor.name: COTAHISTProcessor(),
        BVBG087Processor.name: BVBG087Processor(),
        AnbimaTitpubProcessor.name: AnbimaTitpubProcessor(),
        AnbimaVnaTitpubProcessor.name: AnbimaVnaTitpubProcessor(),
        StockIndexInfoProcessor.name: StockIndexInfoProcessor(),
        DebenturesProcessor.name: DebenturesProcessor(),
        CDIProcessor.name: CDIProcessor(),
    }

    @classmethod
    def build(cls, name):
        return cls.PROCESSORS.get(name)


class MainProcessor:
    def __init__(self, project_id, bucket_id):
        self.project_id = project_id
        self.bucket_id = bucket_id

    def check(self, log):
        logging.info('Checking resourses for %s', log['name'])
        self.processor = ProcessorFactory.build(log['name'])
        if self.processor is None:
            logging.warn('Processor not found: %s', log['name'])
            raise Exception('Processor not found')

        self.storage_client = storage.Client()
        self.filename = 'gs://{}/{}'.format(log['bucket'], log['filename'])
        blob = Blob.from_string(self.filename)
        if not blob.exists(self.storage_client):
            logging.warn(f'File does not exist: {self.filename}')
            raise Exception(f'File does not exist: {self.filename}')
        self.blob = blob

    def parse(self, log):
        logging.info('Parsing %s', log['name'])
        try:
            handle, foutput = tempfile.mkstemp()
            tempf = os.fdopen(handle, 'w+b')
            self.storage_client.download_blob_to_file(self.blob, tempf)
            tempf.flush()
            tempf.seek(0)
            data = self.processor.process(tempf, foutput, log)
        finally:
            logging.info('Cleaning %s', log['name'])
            tempf.close()
            os.remove(foutput)
            logging.info('%s removed', foutput)
        return data

    def save(self, log, data):
        logging.info('Saving Processor %s', self.processor.name)
        output_bucket = storage.Bucket(
            self.storage_client, self.bucket_id, user_project=self.project_id
        )
        gen_files = []
        temp = tempfile.gettempdir()
        for ix in data.keys():
            df = data[ix]
            logging.info('%s shape %s', self.processor.name, df.shape)
            if len(df) == 0:
                logging.warn('empty data frame: %s', self.processor.name)
            fname = os.path.join(temp, ix)
            df.to_parquet(fname)
            refdate = self.processor.format_refdate(
                log['refdate'] or log['filename']
            )
            prefix = '' if ix == 'data' else f'{ix}/'
            output_fname = (
                f'{self.processor.layer1_name}/{prefix}{refdate}.parquet'
            )
            output_blob = Blob(output_fname, output_bucket)
            with open(fname, 'rb') as fp:
                output_blob.upload_from_file(fp)
            logging.info('file saved %s', output_fname)
            gen_files.append(
                {
                    'file_refdate': refdate,
                    'output_fname': output_fname,
                    'size': output_blob.size,
                    'data_rows': df.shape[0],
                    'data_columns': df.shape[1],
                }
            )
        return gen_files

    def process(self, log):
        process_log = {
            'parent': log,
            'time': datetime.now(timezone.utc),
            'processor_name': log['name'],
        }

        try:
            self.check(log)
        except Exception as ex:
            process_log['error'] = str(ex)
            logging.exception(ex)
            logging.error('Problems checking resourses for %s', log['name'])
            return process_log

        try:
            data = self.parse(log)
        except Exception as ex:
            process_log['error'] = str(ex)
            logging.exception(ex)
            logging.error(f'Bad file {self.filename}')
            return process_log

        process_log['results'] = self.save(log, data)
        return process_log
