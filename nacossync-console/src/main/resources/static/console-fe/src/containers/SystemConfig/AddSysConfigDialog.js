import React from 'react'
import {Form, Input, Dialog, ConfigProvider} from '@alifd/next'
import '../../style/dialog-form.scss'

const FormItem = Form.Item

@ConfigProvider.config
class AddSysConfigDialog extends React.Component {
    constructor(props) {
        super(props)
        this.state = {
            visible: false
        }
    }

    save() {
        this.close()
        console.log('save...')
    }

    close() {
        this.setState({visible: false})
    }

    open = () => this.setState({visible: true})

    render() {
        const {locale = {}} = this.props
        return (
            <Dialog
                className="dialog-form"
                title={locale.title}
                visible={this.state.visible}
                onOk={() => this.save()}
                onCancel={() => this.close()}
                onClose={() => this.close()}>
                <Form>
                    <FormItem label={`${locale.configName}:`}>
                        <Input placeholder={locale.configNamePlaceholder}/>
                    </FormItem>
                    <FormItem label={`${locale.configValue}:`}>
                        <Input placeholder={locale.configValuePlaceholder}/>
                    </FormItem>
                </Form>
            </Dialog>
        )
    }
}

export default AddSysConfigDialog
