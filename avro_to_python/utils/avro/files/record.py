""" helper function for generating a record file """
import copy
from typing import List

from avro_to_python.classes.file import File

from avro_to_python.utils.avro.helpers import dedupe_imports

from avro_to_python.utils.avro.types.array import _array_field
from avro_to_python.utils.avro.types.enum import _enum_field
from avro_to_python.utils.avro.types.record import _record_field
from avro_to_python.utils.avro.types.primitive import _primitive_type
from avro_to_python.utils.avro.types.reference import _reference_type
from avro_to_python.utils.avro.types.type_factory import _get_field_type
from avro_to_python.utils.avro.types.union import _union_field
from avro_to_python.utils.avro.types.map import _map_field
from avro_to_python.utils.avro.helpers import _get_namespace


def _record_file(file: File, item: dict, queue: List[dict]) -> None:
    """ function for adding information for record files

    Parameters
    ----------
        file: dict
            file object containing information from the avro schema
        item: dict
            object to be turned into a file
        queue: list
            array of file objects to be processed

    Returns
    -------
        None
    """
    references = []
    for field in item['fields']:

        fieldtype = _get_field_type(
            field=field,
            references=references
        )

        if fieldtype == 'array':
            field = _array_field(
                field=field,
                parent_namespace=file.namespace,
                queue=queue,
                references=references
            )

        elif fieldtype == 'map':
            field = _map_field(
                field=field,
                parent_namespace=file.namespace,
                queue=queue,
                references=references
            )

        # nested complex record
        elif fieldtype == 'record':
            field = _record_field(
                field=field,
                parent_namespace=_get_namespace(field['type'], file.namespace),
                queue=queue,
                references=references
            )

        # nested complex record
        elif fieldtype == 'enum':
            field = _enum_field(
                field=field,
                parent_namespace=_get_namespace(field['type'], file.namespace),
                queue=queue,
                references=references
            )

        # handle union type
        elif fieldtype == 'union':
            field = _union_field(
                field=field,
                parent_namespace=file.namespace,
                queue=queue,
                references=references
            )

        elif fieldtype == 'reference':
            field = _prepare_field_for_reference(field)
            field = _reference_type(
                field=field,
                references=references
            )

        # handle primitive types
        elif fieldtype == 'primitive':
            field = _primitive_type(field)

        else:
            raise ValueError('fieldtype is not supported...')

        file.fields[field.name] = field
        file.imports += references

    file.imports = dedupe_imports(file.imports)


def _prepare_field_for_reference(field: dict) -> dict:
    """parse the field dictionary and return one that will create a meaningful reference object

    there are basicaly two ways we encounter references at this stage
        field = {'name': 'RecordClass', 'type': 'path.to.containing.namespace'}
        field = {'name': 'fieldName', 'type': 'path.to.containing.namespace.RecordClass'}

    I *think* the reason we encounter the latter is due to a malformed `avsc` file (which in turn I
    *think* happens due to a bug in the job that converts avdl -> avsc). Either way, we try to
    handle that before we attempt to create a reference
    """
    ref_field = copy.deepcopy(field)
    name = ref_field['name']
    typ = ref_field['type']
    # bad proxies for the behavior I'm talking about above
    name_is_alias = name[0].islower()
    type_is_record_class = typ.count('.') > 0 and not typ.islower()
    if name_is_alias and type_is_record_class:
        *new_type, new_name = typ.split('.')
        new_type = '.'.join(new_type)
        ref_field['name'] = new_name
        ref_field['type'] = new_type
    return ref_field
