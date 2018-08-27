#!/usr/bin/env python

import argparse
import getpass
import sys


def create_user(opts):
    from airflow.contrib.auth.backends.password_auth import PasswordUser
    from airflow import models, settings

    u = PasswordUser(models.User())
    u.username = opts['username']
    u.email = opts['email']
    u._set_password = opts['password']

    s = settings.Session()
    s.add(u)
    s.commit()
    s.close()

new_user = {
    'username': 'Big Squid',
    'email': 'bcooper@bigsquid.com',
    'password': 'Bigsquid1!'
}

create_user(new_user)
