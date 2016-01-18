Image Worker Example
====================

This is a simple example implementation of a Python process that can perform various image manipulations.
It is meant to be used as a worker, watching a queue for new work.
Planning to utilize it for a online course I am developing on Udemy.

The associated Image Upload Web App can be found `here <https://github.com/samkeen/work-queue-backed-image-upload-example>`_

Implementation Notes
********************

Tested on Python ``3.5.x``

Using the `Pillow library <http://pillow.readthedocs.org/en/3.0.x/handbook/tutorial.html>`_ for the actual image
manipulations.

Usage
-----

``WORK_QUEUE_NAME=<your SQS name> S3_BUCKET_NAME=<the S3 bucket for image storage> python worker.py``
