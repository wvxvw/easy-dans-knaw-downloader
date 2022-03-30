# -*- coding: utf-8 -*-

import queue
import time
import logging
import json
import os
import subprocess

from argparse import ArgumentParser
from multiprocessing import Process, Queue, Event
from subprocess import Popen

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

parser.add_argument(
    '-D',
    '--directory',
    default='Data',
    help='''
    Directory to fetch.
    ''',
)


class WebDriverProcess(Process):

    def __init__(self, node, sink, jobs, url, out_dir, fetch_dir, timeout=1):
        super().__init__()
        self.node = node
        self.node_url = 'http://{}:5555/wd/hub'.format(node)
        self.sink = sink
        self.jobs = jobs
        self.out_dir = out_dir
        self.url = url
        self.fetch_dir = fetch_dir
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

        # these links seem to have ids, but they also seem unreliable
        known_directories = {
            'Data': (
                '/html/body/div/div[2]/div/div[2]/div[2]/div[1]/div/'
                'div/div/div[2]/div[2]/div[1]/div[1]/div/div/div[2]/'
                'div[1]/div/span[2]/a'
            ),
            'History': (
                '/html/body/div/div[2]/div/div[2]/div[2]/div[1]/div/'
                'div/div/div[2]/div[2]/div[1]/div[1]/div/div/div[2]/'
                'div[2]/div/span[2]/a'
            ),
        }
        data_dir = known_directories[self.fetch_dir]
        data_dir_button = self.driver.find_element_by_xpath(data_dir)
        data_dir_button.click()

        location = (
            '/html/body/div/div[2]/div/div[2]/div[2]/div[1]/div/'
            'div/div/div[2]/div[1]/div[2]/ol/li[2]/span'
        )

        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, location),
            ))
        WebDriverWait(self.driver, 10).until(
            EC.text_to_be_present_in_element(
                (By.XPATH, location),
                self.fetch_dir,
            ))
        # It's not really possible to tell if the talbe finished
        # loading at this point
        time.sleep(5)

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

            dl_file = os.path.join(self.out_dir, dl_button.text.strip())
            logging.info(
                '{} Waiting for download to start: {}'.format(
                    self.node,
                    dl_file,
                ))

            for _ in range(60):
                if os.path.isfile(dl_file):
                    logging.info(
                        '{} Download started: {}'.format(self.node, dl_file),
                    )
                    break
                else:
                    time.sleep(1)
            else:
                logging.error(
                    '{} Download failed to start: {}'.format(
                        self.node,
                        dl_file,
                    ))
                return False

            holders = Popen(
                ('lsof', dl_file),
                stderr=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
            )
            holders.wait()
            holders_out = holders.stdout.read().strip()
            while holders_out:
                logging.info(
                    '{} Waiting for download to complete: {}'.format(
                        self.node,
                        dl_file,
                    ))
                time.sleep(1)
                holders = Popen(
                    ('lsof', dl_file),
                    stderr=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                )
                holders.wait()
                holders_out = holders.stdout.read().strip()
                logging.info(
                    '{} Waiting for {} to finish'.format(
                        self.node,
                        holders_out.decode(),
                    ))

            logging.info(
                '{} Download completed: {}'.format(
                    self.node,
                    dl_file,
                ))
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
            pargs.directory,
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
