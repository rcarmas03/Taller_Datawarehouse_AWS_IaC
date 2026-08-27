#!/usr/bin/env python3
import os

import aws_cdk as cdk

from taller_datawarehouse_aws_ia_c.taller_datawarehouse_aws_ia_c_stack import TallerDatawarehouseAwsIaCStack


app = cdk.App()
TallerDatawarehouseAwsIaCStack(
    app, 
    "TallerDatawarehouseAwsIaCStack",                           
    )

app.synth()
