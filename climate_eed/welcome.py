# -------------------------------------------------------------------------------
# Licence:
# Copyright (c) 2012-2022 Luzzi Valerio
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
#
# Name:        stime.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     14/04/2022
# -------------------------------------------------------------------------------
import click
import pkg_resources
from .filesystem import juststem


def get_version():
    """
    get_version
    """
    package_name = juststem(__name__)
    dist = pkg_resources.get_distribution(package_name)
    return dist.version


def welcome(verbose=False):
    """
    welcome
    """
    if verbose:
        version = get_version()
        text = f"""
        +-------------------------------------------------------------------------------------------------+
        |                                                                                                 |
        |       Climate EED Interface version {version}
        |                                                                                                 |
        +-------------------------------------------------------------------------------------------------+
        """
        click.echo(text)
