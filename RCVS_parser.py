try:
    import xml.etree.cElementTree as eT
except ImportError:
    import xml.etree.ElementTree as eT
import unicodecsv as csv
from dlcs.image_collection import Image, ImageCollection
from parser_output import Work, ParserResponse
from collections import OrderedDict
import settings
import logging


class Parser:
    def __init__(self, space=1):
        pass
        self.space = space

    def parse(self, original_filename, parse_file):

        logging.debug("parsing " + original_filename + " as " + parse_file)
        work = Work()
        toc = OrderedDict()
        flags = {}

        work.id = original_filename.rsplit('.', 1)[0][4:]

        with open(parse_file, 'rb') as csv_file:

            reader = csv.DictReader(csv_file, dialect='excel-tab', delimiter='\t', encoding='utf-8-sig')
            images = []
            image_metadata = {}

            if original_filename.startswith('lib'):
                self.parse_library_data(reader, work, images, image_metadata, toc, flags)
            elif original_filename.startswith('arc'):
                self.parse_archive_data(reader, work, images, image_metadata, toc, flags)

            work.image_collection = ImageCollection(images)
            work.toc = toc
            work.image_metadata = image_metadata
            work.flags = flags

        response = ParserResponse()
        response.works = [work]
        response.collections = []

        return response

    def parse_archive_data(self, reader, work, images, image_metadata, toc, flags):

        first = True
        image_index = 0
        for row in reader:
            if first:
                work.label = row[ArcWorkTitleColumn]
                work.work_metadata = self.get_metadata_for_cols(row,
                                                                [
                                                                    ArcCatalogueRefColumn,
                                                                    ArcWorkTitleColumn,
                                                                    ArcRepositoryColumn,
                                                                    ArcCollectionColumn,
                                                                    ArcSeriesColumn,
                                                                    ArcSubSeriesColumn,
                                                                    ArcCopyrightColumn,
                                                                    ArcPermalinkColumn
                                                                ])
                viewing_mode = row[ViewingModeColumn]
                flags['Viewing_Mode'] = viewing_mode
                first = False
            else:

                origin = settings.RCVS_RELATIVE + row.get(ArcFilenameColumn)
                image = Image(space=settings.CURRENT_SPACE, origin=origin, string_1=work.id, number_1=0, number_2=image_index)
                images.append(image)

                # toc
                article_entries = map(lambda x: x.strip(), row.get(LibContentsColumn).split('|'))
                for article in article_entries:
                    if article not in toc:
                        toc[article] = []
                    toc[article].append(image_index)

                image_metadata[image_index] = self.get_metadata_for_cols(row,
                                                                         [
                                                                             ArcCatalogueRefColumn,
                                                                             ArcImageTitleColumn,
                                                                             ArcDateColumn,
                                                                             ArcDescriptionColumn,
                                                                             ArcCreatorColumn,
                                                                             ArcFormatColumn,
                                                                             ArcCatalogueEntryURLColumn
                                                                         ])

                image_index += 1

    def parse_library_data(self, reader, work, images, image_metadata, toc, flags):

        first = True
        image_index = 0
        for row in reader:
            if first:
                work.label = row[LibWorkTitleColumn]
                work.work_metadata = self.get_metadata_for_cols(row,
                                                                [
                                                                    LibWorkTitleColumn,
                                                                    LibRepositoryColumn,
                                                                    LibCollectionColumn,
                                                                    LibVolumeColumn,
                                                                    LibChapterColumn,
                                                                    LibIssueColumn,
                                                                    LibDateColumn,
                                                                    LibPublicationInfoColumn,
                                                                    LibMaterialTypeColumn,
                                                                    LibGeneralNoteColumn,
                                                                    LibLanguageColumn,
                                                                    LibCopyrightColumn,
                                                                    LibPermalinkColumn
                                                                ])
                viewing_mode = row[ViewingModeColumn]
                flags['Viewing_Mode'] = viewing_mode
                flags['Canvas_Label_Field'] = 'Page'
                first = False
            else:

                # images
                origin = settings.RCVS_RELATIVE + row.get(LibFilenameColumn)
                image = Image(space=settings.CURRENT_SPACE, origin=origin, string_1=work.id, number_1=0, number_2=image_index)
                images.append(image)

                # toc
                article_entries = map(lambda x: x.strip(), row.get(LibContentsColumn, '').split('|'))
                for article in article_entries:
                    if article is not None and len(article) > 0:
                        if article not in toc:
                            toc[article] = []
                        toc[article].append(image_index)

                # metadata
                image_metadata[image_index] = self.get_metadata_for_cols(row,
                                                                         [
                                                                             LibPageColumn,
                                                                             LibArticleColumn,
                                                                             LibAuthorColumn,
                                                                             LibSubjectColumn,
                                                                             LibCatalogueEntryURLColumn
                                                                         ])
                image_index += 1

    @staticmethod
    def get_metadata_for_cols(row, cols):

        meta = []

        for column in cols:
            value = row.get(column)
            if value is not None and len(value) > 0:
                meta.append({'label': column, 'value': value})

        return meta

    def get_manifest_path_from_reference(self, reference):

        return 'http://dlcs.io/iiif-resource/50/waylon-rcdd/' + reference + '/0'

    def get_images_for_work_path(self, reference):

        return 'http://dlcs.io/raw-resource/50/waylon-rcdd/' + reference + '/0'

    def custom_decoration(self, data, manifest):

        flags = data.get('flags')
        if flags is not None:
            mode = flags.get('Viewing_Mode')
            if mode is not None and mode == "2":
                manifest['sequences'][0]['viewingHint'] = 'paged'

        manifest['logo'] = "http://uv.rcvsvethistory.org/RCVS_Knowledge_Logo_whiteout.png"

        manifest['attribution'] = "<a href='http://www.rcvsvethistory.org/'>RCVS Vet History</a> brought to you by RCVS Knowledge"

# --Column Mappings-- #

LibContentsColumn = 'Contents'
ViewingModeColumn = 'Viewing Mode'

# Library data columns names - work
LibFilenameColumn ='File name'
LibWorkTitleColumn = 'Work Title'
LibRepositoryColumn = "Repository"
LibCollectionColumn = 'Collection'
LibVolumeColumn = 'Volume'
LibChapterColumn = 'Chapter'
LibIssueColumn = 'Issue'
LibDateColumn = 'Date'
LibPublicationInfoColumn = 'Publication Info'
LibMaterialTypeColumn = 'Material Type'
LibGeneralNoteColumn = 'General Note'
LibLanguageColumn = 'Language'
LibCopyrightColumn = 'Copyright'
LibPermalinkColumn = 'Permalink'

# Library data columns names - image
LibPageColumn = 'Page'
LibArticleColumn = 'Article'
LibAuthorColumn = 'Author'
LibSubjectColumn = 'Subject'
LibCatalogueEntryURLColumn = 'Catalogue Entry URL'

# archive data column names
ArcFilenameColumn = 'File name'
ArcWorkTitleColumn = 'Work Title'
ArcRepositoryColumn = 'Repository'
ArcCollectionColumn = 'Collection'
ArcSeriesColumn = 'Series'
ArcSubSeriesColumn = 'Subseries'
ArcCatalogueRefColumn = 'Catalogue ref'
ArcCopyrightColumn = 'Copyright'
ArcPermalinkColumn = "Permalink"

# archive data column names - image
ArcImageCatalogueRefColumn = 'Catalogue ref'
ArcImageTitleColumn = 'Title'
ArcDateColumn = 'Date'
ArcDescriptionColumn = 'Description'
ArcCreatorColumn = 'Creator'
ArcFormatColumn = 'Format'
ArcCatalogueEntryURLColumn = 'Catalogue Entry URL'



