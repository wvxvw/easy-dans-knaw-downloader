# -*- coding: utf-8 -*-

import queue
import time
import logging
import json

from argparse import ArgumentParser
from multiprocessing import Process, Queue, Event

from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains


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


def tx_to_url(tx):
    return 'https://www.blockchain.com/btc/tx/{}'.format(tx)


class WebDriverProcess(Process):

    def __init__(self, node, sink, jobs, is_done, timeout=1):
        super().__init__()
        self.node = node
        self.sink = sink
        self.jobs = jobs
        self.is_done = is_done
        self.timeout = timeout

    def run(self):
        self.driver = webdriver.Remote(
            command_executor=self.node,
            desired_capabilities=DesiredCapabilities.CHROME,
        )
        self.driver.maximize_window()
        while not self.is_done.is_set():
            try:
                tx = self.jobs.get_nowait()
                self.process_url(tx_to_url(tx))
            except queue.Empty:
                time.sleep(self.timeout)

    def process_url(self, url):
        logging.info('Processing: {}'.format(url))
        src_tx = url.split('/')[-1]
        self.driver.get(url)
        txs = self.find_all_transactions()
        logging.info('Found {} transactions'.format(len(txs)))
        if not txs:
            self.sink.put((src_tx, 'coinbase'))
        else:
            for tx in txs:
                self.sink.put((src_tx, tx))

    def find_all_transactions(self):
        extras_path = (
            '/html/body/div[1]/div[3]/div/div[3]/div/div[3]/'
            'div[2]/div[1]/div[2]/div/div[2]/a'
        )
        while True:
            expand = self.driver.find_elements_by_xpath(extras_path)
            if expand:
                message = expand[0].text
                remaining = int(message.split()[-2][1:])
                logging.info('Expanding: {}'.format(message))
                elt = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, extras_path),
                    ))
                actions = ActionChains(self.driver)
                desired_y = (elt.size['height'] / 2) + elt.location['y']
                middle = self.driver.execute_script('return window.innerHeight') / 2
                current_y = middle + self.driver.execute_script('return window.pageYOffset')
                scroll_y_by = desired_y - current_y
                self.driver.execute_script('window.scrollBy(0, arguments[0]);', scroll_y_by)
                actions.click(elt).perform()
                remaining -= 10
                if remaining > 0:
                    WebDriverWait(self.driver, 10).until(
                        EC.text_to_be_present_in_element(
                            (By.XPATH, extras_path),
                            'Load more inputs... ({} remaining)'.format(remaining),
                        ))
            else:
                break
        tx_path = (
            '//*[@id="__next"]/div[3]/div/div[3]/div/div[3]/'
            'div[2]/div[1]/div[2]/div/div[1]/div/div/a'
        )
        return [
            a.get_attribute('href').split('/')[-1]
            for a in self.driver.find_elements_by_xpath(tx_path)
        ]


def all_paths_found(paths):
    return all(paths.values())


def update_path(paths, src_tx, tx):
    paths[src_tx] = tx
    if tx not in paths:
        paths[tx] = None
        return True
    return False


def scrap(argsv):
    pargs = parser.parse_args(argsv)

    logging.basicConfig(
        force=True,
        level=pargs.verbosity,
    )

    sink = Queue()
    jobs = Queue()
    is_done = Event()
    workers = [
        WebDriverProcess(
            'http://{}:5555/wd/hub'.format(node),
            sink,
            jobs,
            is_done,
        )
        for node in pargs.node
    ]

    for worker in workers:
        worker.start()

    try:
        paths = {pargs.transaction: None}
        jobs.put_nowait(pargs.transaction)
        
        while not all_paths_found(paths):
            try:
                src_tx, tx = sink.get()

                if update_path(paths, src_tx, tx):
                    if tx != 'coinbase':
                        jobs.put_nowait(tx)
            except queue.Empty:
                time.sleep(1)

        is_done.set()
        
        if not pargs.output:
            print(json.dumps(paths, indent=4))
        else:
            with open(pargs.output, 'w') as f:
                json.dump(paths, f, indent=4)
    finally:
        for worker in workers:
            worker.terminate()


def main(argsv):
    try:
        scrap(argsv)
    # TODO(wvxvw): This needs to be more specific about errors
    except Exception as e:
        logging.exception(e)
        return 1
    return 0
