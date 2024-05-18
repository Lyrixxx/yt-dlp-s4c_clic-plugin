from __future__ import unicode_literals
from yt_dlp.extractor.common import InfoExtractor
from yt_dlp.utils import (
    ExtractorError,
    int_or_none,
    try_get,
)
import re
import datetime

# Map Welsh months to numbers
WELSH_MONTHS = {
    'Ionawr': '01',
    'Chwefror': '02',
    'Mawrth': '03',
    'Ebrill': '04',
    'Mai': '05',
    'Mehefin': '06',
    'Gorffennaf': '07',
    'Awst': '08',
    'Medi': '09',
    'Hydref': '10',
    'Tachwedd': '11',
    'Rhagfyr': '12'
}

def convert_welsh_date(welsh_date):
    day, month_welsh, year = welsh_date.split()
    month = WELSH_MONTHS[month_welsh]
    return f"{year}-{month}-{day}"

def parse_welsh_date(welsh_date):
    return int(datetime.datetime.strptime(convert_welsh_date(welsh_date), "%Y-%m-%d").timestamp())

def format_date(timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y%m%d')

class S4CClicBaseIE(InfoExtractor):
    _BASE_URL = 'https://www.s4c.cymru/clic/'
    _API_URL = 'https://www.s4c.cymru/df/'
    _PLAYER_API_URL = 'https://player-api.s4c-cdn.co.uk/player-configuration/prod'
    _STREAMING_URL_API = 'https://player-api.s4c-cdn.co.uk/streaming-urls/prod'

    def _real_extract(self, url):
        video_id = self._match_id(url)
        return self._extract_video_info(video_id)

    def _extract_video_info(self, video_id):
        raise NotImplementedError('This method must be implemented by subclasses')

    def _extract_episode_number_from_title(self, title):
        mobj = re.match(r'^(?:Pennod|Episode)\s*(\d+)', title)
        if mobj:
            return int(mobj.group(1))
        return None

    def _extract_episode_number_from_text(self, text):
        mobj = re.match(r'^(\d+)[;:,\-_\.\s]*', text)
        if mobj:
            return int(mobj.group(1)), re.sub(r'^\d+[;:,\-_\.\s]*', '', text).strip()
        return None, text

    def _extract_season_number_from_title(self, title):
        # First, try to match with separators
        mobj = re.search(r'\s*[-:;,_\.\s]*\s*(Cyfres|Season)\s*X?(\d+)', title, re.IGNORECASE)
        if mobj:
            season_number = int(mobj.group(2))
            title = title.replace(mobj.group(0), '').strip()
            return season_number, title

        # Fallback to match without separators
        mobj = re.search(r'(Cyfres|Season)\s*X?(\d+)', title, re.IGNORECASE)
        if mobj:
            season_number = int(mobj.group(2))
            title = title.replace(mobj.group(0), '').strip()
            return season_number, title

        return None, title

    def _extract_episode_number(self, text):
        mobj = re.search(r'\bE(\d+)\b', text)
        if mobj:
            return int(mobj.group(1))
        return None

    def _extract_episode_number_from_filename(self, filename):
        mobj = re.search(r'[_-](?:pennod|episode)_?(\d+)', filename, re.IGNORECASE)
        if mobj:
            return int(mobj.group(1))
        return None

    def _extract_episode_number_from_thumbnail(self, thumbnail_url):
        mobj = re.search(r'_P(\d+)', thumbnail_url)
        if mobj:
            return int(mobj.group(1))
        return None

    def _fetch_and_validate_streaming_urls(self, filename, programme_id):
        formats = []
        for region in ['WW', 'UK']:
            streaming_urls = self._download_json(
                f'{self._STREAMING_URL_API}?mode=od&application=clic&region={region}&extra=false&thirdParty=false&filename={filename}',
                programme_id,
                fatal=False
            )
            if not streaming_urls:
                continue
            formats.extend(self._extract_m3u8_formats(
                streaming_urls.get('hls'), filename, 'mp4', 'm3u8_native',
                m3u8_id=f'hls-{region}', fatal=False))
            formats.extend(self._extract_mpd_formats(
                streaming_urls.get('dash'), filename, mpd_id=f'dash-{region}', fatal=False))
            formats.extend(self._extract_mpd_formats(
                streaming_urls.get('fvp'), filename, mpd_id=f'fvp-{region}', fatal=False))
            formats.extend(self._extract_mpd_formats(
                streaming_urls.get('dvb'), filename, mpd_id=f'dvb-{region}', fatal=False))
        return formats

    def _extract_episode_info(self, episode, programme_id):
        series_title = episode.get('series_title') or episode.get('programme_title', 'No Title')
        description = episode.get('full_billing', 'No Description')
        duration = int(episode.get('duration', 0)) * 60  # Convert minutes to seconds

        # Parse the title and extract season number if available
        season_number, title = self._extract_season_number_from_title(series_title)
        programme_title = episode.get('programme_title', '')
        episode_number = self._extract_episode_number_from_title(programme_title)
        if episode_number is None:
            episode_number, programme_title = self._extract_episode_number_from_text(programme_title)
        episode_title = programme_title

        # Use series title if no specific episode title
        if not episode_title:
            episode_title = title

        # Extract episode number from filename and thumbnail URL if not found
        mpg_filename = episode.get('mpg', '')
        if episode_number is None:
            episode_number = self._extract_episode_number(mpg_filename)

        thumbnail_url = episode.get('thumbnail_url', '')
        if episode_number is None:
            episode_number = self._extract_episode_number_from_thumbnail(thumbnail_url)

        # Extract episode number from filename
        if episode_number is None and mpg_filename:
            episode_number = self._extract_episode_number_from_filename(mpg_filename)

        # Fetch additional data including filename and poster
        config_url = f'{self._PLAYER_API_URL}?programme_id={programme_id}&signed=0&lang=cy&mode=od&appId=clic&streamName=&env=live'
        config_data = self._download_json(config_url, programme_id, note='Downloading player configuration')

        if not config_data:
            raise ExtractorError('Unable to fetch player configuration')

        mpg_filename = config_data.get('filename')
        if not mpg_filename:
            raise ExtractorError('Filename is missing in the player configuration')
        thumbnail = config_data.get('poster')

        formats = self._fetch_and_validate_streaming_urls(mpg_filename, programme_id)

        if not formats:
            raise ExtractorError('Unable to fetch valid streaming URLs')

        # Handle subtitles
        subtitles = {}
        subtitles_list = config_data.get('subtitles', [])
        for subtitle in subtitles_list:
            lang_code = subtitle.get('3')
            vtt_url = subtitle.get('0')
            if lang_code and vtt_url:
                subtitles.setdefault(lang_code, []).append({
                    'url': vtt_url,
                    'ext': 'vtt',
                })

        # Extract dates
        last_tx = episode.get('last_tx')
        clic_aired = episode.get('clic_aired')
        release_timestamp, release_date, release_year, modified_timestamp, modified_date = None, None, None, None, None

        if last_tx:
            release_timestamp = parse_welsh_date(last_tx)
            release_date = format_date(release_timestamp)
            release_year = int(release_date[:4])

        if clic_aired:
            modified_timestamp = parse_welsh_date(clic_aired)
            modified_date = format_date(modified_timestamp)

        # Default season number to 0 if not set
        if season_number is None:
            season_number = 0

        return {
            'id': programme_id,
            'title': title,
            'description': description,
            'thumbnail': thumbnail,
            'duration': duration,
            'formats': formats,
            'subtitles': subtitles,
            'series': title,
            'series_id': episode.get('series_id'),
            'season_number': season_number,
            'episode': episode_title,
            'episode_number': episode_number,
            'episode_id': programme_id,
            'timestamp': release_timestamp,
            'upload_date': release_date,
            'release_timestamp': release_timestamp,
            'release_date': release_date,
            'release_year': release_year,
            'modified_timestamp': modified_timestamp,
            'modified_date': modified_date,
        }


class S4CClicSeriesIE(S4CClicBaseIE):
    IE_NAME = 's4c:clic:series'
    _VALID_URL = r'https?://(?:www\.)?s4c\.cymru/clic/series/(?P<id>\d+)'

    def _extract_video_info(self, series_id):
        series_data = self._download_json(
            f'{self._API_URL}series_details?lang=c&series_id={series_id}&show_prog_in_series=Y', 
            series_id
        )
        if not series_data.get('other_progs_in_series'):
            return self._extract_episode_info(series_data['full_prog_details'][0], series_id)

        entries = [
            self.url_result(
                f'{self._BASE_URL}programme/{episode["id"]}', 
                ie=S4CClicProgrammeIndividualIE.ie_key(), 
                video_id=episode["id"]
            ) for episode in series_data.get('full_prog_details', []) + series_data.get('other_progs_in_series', [])
        ]
        return self.playlist_result(entries, series_id, try_get(series_data, lambda x: x['full_prog_details'][0]['series_title'], str))

class S4CClicProgrammeIE(S4CClicBaseIE):
    IE_NAME = 's4c:clic:programme'
    _VALID_URL = r'https?://(?:www\.)?s4c\.cymru/clic/programme/(?P<id>\d+)'

    def _extract_video_info(self, programme_id):
        programme_data = self._download_json(
            f'{self._API_URL}full_prog_details?lang=c&programme_id={programme_id}&show_prog_in_series=Y',
            programme_id
        )
        programme = programme_data['full_prog_details'][0]
        series_id = programme.get('series_id')

        if not programme_data.get('other_progs_in_series'):
            return self._extract_episode_info(programme, programme_id)

        # Get the series data to fetch all episodes in the series
        series_data = self._download_json(
            f'{self._API_URL}series_details?lang=c&series_id={series_id}&show_prog_in_series=Y', 
            series_id
        )
        entries = [
            self.url_result(
                f'{self._BASE_URL}programme/{episode["id"]}', 
                ie=S4CClicProgrammeIndividualIE.ie_key(), 
                video_id=episode["id"]
            ) for episode in series_data.get('full_prog_details', []) + series_data.get('other_progs_in_series', [])
        ]
        return self.playlist_result(entries, series_id, try_get(series_data, lambda x: x['full_prog_details'][0]['series_title'], str))

class S4CClicProgrammeIndividualIE(S4CClicBaseIE):
    IE_NAME = 's4c:clic:programme:individual'
    _VALID_URL = r'https?://(?:www\.)?s4c\.cymru/clic/programme/(?P<id>\d+)'

    def _extract_video_info(self, programme_id):
        programme_data = self._download_json(
            f'{self._API_URL}full_prog_details?lang=c&programme_id={programme_id}&show_prog_in_series=Y',
            programme_id
        )
        return self._extract_episode_info(programme_data['full_prog_details'][0], programme_id)

__all__ = ['S4CClicSeriesIE', 'S4CClicProgrammeIE', 'S4CClicProgrammeIndividualIE']
