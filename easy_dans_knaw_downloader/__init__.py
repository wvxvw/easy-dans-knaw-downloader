# -*- coding: utf-8 -*-

import queue
import time
import logging
import json
import os

from argparse import ArgumentParser
from multiprocessing import Process, Queue, Event

from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException


# TODO(wvxvw): Also configure logging
parser = ArgumentParser('easy.dans.knaw.nl downloader')
parser.add_argument(
    '-o',
    '--output',
    default=None,
    help='''
    Store scrapper's output in this directory
    (default: current directory)
    ''',
)
parser.add_argument(
    '-n',
    '--node',
    action='append',
    default=[],
    help='''
    Selenium worker node URL
    ''',
)
parser.add_argument(
    '-v',
    '--verbosity',
    default=logging.WARNING,
    type=int,
    help='''
    Logging verbosity level. Default 30 (WARNING).
    ''',
)
parser.add_argument(
    '-d',
    '--dataset',
    help='''
    Dataset id to download (appears in the URL of the site):
    Example: 
    https://easy.dans.knaw.nl/ui/datasets/id/easy-dataset:112935
    112935 is the id.
    ''',
)


class WebDriverProcess(Process):

    def __init__(self, node, sink, jobs, url, out_dir, timeout=1):
        super().__init__()
        self.node = node
        self.node_url = 'http://{}:5555/wd/hub'.format(node)
        self.sink = sink
        self.jobs = jobs
        self.out_dir = out_dir
        self.url = url
        self.timeout = timeout
        try:
            os.makedirs(self.out_dir)
        except FileExistsError:
            pass

    def run(self):
        options = webdriver.ChromeOptions()
        logging.info(
            '{} Will store files in {}'.format(self.node, self.out_dir),
        )
        prefs = {
            'download.default_directory': '/home/selenium/downloads',
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing.enabled': True,
            'profile.default_content_setting_values.automatic_downloads': 2,
        }
        options.add_experimental_option('prefs', prefs)
        self.driver = webdriver.Remote(
            command_executor=self.node_url,
            desired_capabilities=DesiredCapabilities.CHROME,
            options=options,
        )
        logging.info('Starting: {}'.format(self.node))
        self.driver.get(self.url)
        logging.info('{} Visited: {}'.format(self.node, self.url))
        self.driver.maximize_window()

        data_dir = (
            '/html/body/div/div[2]/div/div[2]/div[2]/div[1]/div/'
            'div/div/div[2]/div[2]/div[1]/div[1]/div/div/div[2]/'
            'div[1]/div/span[2]/a'
        )
        data_dir_button = self.driver.find_element_by_xpath(data_dir)
        data_dir_button.click()

        location = (
            '/html/body/div/div[2]/div/div[2]/div[2]/div[1]/div/'
            'div/div/div[2]/div[1]/div[2]/ol/li[2]/span'
        )

        location_ready = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, location),
            ))

        while True:
            try:
                tx = self.jobs.get_nowait()
                logging.info('{} Accepted: {}'.format(self.node, tx))
                if not self.process_item(tx):
                    break
            except queue.Empty:
                time.sleep(self.timeout)

    def table_item(self, item):
        return (
            '/html/body/div/div[2]/div/div[2]/div[2]/div[1]/div/'
            'div/div/div[2]/div[2]/div[2]/table/tbody/tr[{}]/td[2]/'
            'span/a'
        ).format(item + 1)

    def process_item(self, item):
        logging.info('{} Processing: {}'.format(self.node, item))
        response = self.download_item(item)
        logging.info('{} Downloaded {}'.format(self.node, item))
        self.sink.put((item, self.node, response))
        return response

    def download_item(self, item):
        dl_path = self.table_item(item)
        try:
            dl_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, dl_path),
                ))
            logging.info(
                '{} Clicking download button: {}'.format(
                    self.node,
                    dl_button.text,
                ))
            dl_button.click()
            return True
        except TimeoutException:
            return False


def scrap(argsv):
    pargs = parser.parse_args(argsv)

    logging.basicConfig(
        force=True,
        level=pargs.verbosity,
    )

    logging.info('Scrapper started')
    logging.info('Dataset: {}'.format(pargs.dataset))
    logging.info('Nodes: {}'.format(pargs.node))
    logging.info('Output: {}'.format(pargs.output))

    sink = Queue()
    jobs = Queue()
    workers = {
        node: WebDriverProcess(
            node,
            sink,
            jobs,
            (
                'https://easy.dans.knaw.nl/ui/datasets/'
                'id/easy-dataset:{}/tab/2'.format(pargs.dataset)
            ),
            pargs.output,
        )
        for node in pargs.node
    }

    for worker in workers.values():
        worker.start()

    done = []

    try:
        for dfile, worker in enumerate(workers.values()):
            jobs.put_nowait(dfile)

        while workers:
            item, node, response = sink.get()
            logging.info('{} Notified: {} {}'.format(node, item, response))

            if not response:
                done.append(workers[node])
                del workers[node]
            else:
                dfile += 1
                jobs.put_nowait(dfile)

        logging.info('Finished downloading.')
    finally:
        for worker in done:
            worker.terminate()
        for worker in workers.values():
            worker.terminate()


def main(argsv):
    try:
        scrap(argsv)
    # TODO(wvxvw): This needs to be more specific about errors
    except Exception as e:
        logging.exception(e)
        return 1
    return 0
