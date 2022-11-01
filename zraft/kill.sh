#!/bin/bash

ps -ef | grep zrf | awk '{print $2}' | xargs kill -9
