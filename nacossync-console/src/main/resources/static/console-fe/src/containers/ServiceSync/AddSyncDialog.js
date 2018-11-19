import React from 'react'
import {Form, Input, Select, Dialog, ConfigProvider} from '@alifd/next'
import '../../style/dialog-form.scss'

const FormItem = Form.Item

@ConfigProvider.config
class AddSyncDialog extends React.Component {
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
                    <FormItem label={`${locale.serviceName}:`}>
                        <Input placeholder={locale.serviceNamePlaceholder}/>
                    </FormItem>
                    <FormItem label={`${locale.groupName}:`}>
                        <Input placeholder={locale.groupNamePlaceholder}/>
                    </FormItem>
                    <FormItem label={`${locale.sourceCluster}:`}>
                        <Select>
                            <Select.Option value="option1">CS</Select.Option>
                        </Select>
                    </FormItem>
                    <FormItem label={`${locale.destCluster}:`}>
                        <Select>
                            <Select.Option value="option1">Nacos</Select.Option>
                        </Select>
                    </FormItem>
                </Form>
            </Dialog>
        )
    }
}

export default AddSyncDialog
