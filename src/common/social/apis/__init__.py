
from .. facebook import FacebookAPI
from .. google import GoogleAPI
from .. steam import SteamAPI
from .. vk import VKAPI
from .. xsolla import XsollaAPI
from .. mailru import MailRuAPI


api_types = {
    FacebookAPI.NAME: FacebookAPI,
    GoogleAPI.NAME: GoogleAPI,
    SteamAPI.NAME: SteamAPI,
    VKAPI.NAME: VKAPI,
    XsollaAPI.NAME: XsollaAPI,
    MailRuAPI.NAME: MailRuAPI
}
