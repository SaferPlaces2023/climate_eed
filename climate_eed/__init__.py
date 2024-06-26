# -------------------------------------------------------------------------------
# Licence:
# Copyright (c) 2012-2024 Renzi Marco 
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
# Name:        __init__.py.py
# Purpose:
#
# Author:      Renzi Marco
#
# Created:     19/03/2024
# -------------------------------------------------------------------------------
from .main import main
from .module_commands import fetch_var_planetary, fetch_var_copernicus, fetch_var_smhi, list_repo_vars
from .module_config import PlanetaryConfig