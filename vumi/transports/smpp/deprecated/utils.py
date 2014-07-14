from vumi.transports.smpp.processors import (
    DeliveryReportProcessorConfig, SubmitShortMessageProcessorConfig,
    DeliverShortMessageProcessorConfig)


def convert_to_new_config(config,
                          dr_processor,
                          submit_sm_processor,
                          deliver_sm_processor):
    dr_config = dict(
        (field.name, config.pop(field.name))
        for field in DeliveryReportProcessorConfig._get_fields()
        if field.name in config)

    submit_sm_config = dict(
        (field.name, config.pop(field.name))
        for field in SubmitShortMessageProcessorConfig._get_fields()
        if field.name in config)

    deliver_sm_config = dict(
        (field.name, config.pop(field.name))
        for field in DeliverShortMessageProcessorConfig._get_fields()
        if field.name in config)

    config.update({
        'delivery_report_processor': dr_processor,
        'delivery_report_processor_config': dr_config,
        'submit_short_message_processor': submit_sm_processor,
        'submit_short_message_processor_config': submit_sm_config,
        'deliver_short_message_processor': deliver_sm_processor,
        'deliver_short_message_processor_config': deliver_sm_config,
    })

    return config
