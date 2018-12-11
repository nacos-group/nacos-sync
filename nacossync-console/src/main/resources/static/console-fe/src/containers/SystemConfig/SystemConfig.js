import React from 'react'
import {Table, Pagination, Form, Input, Button, Dialog, Message, ConfigProvider} from '@alifd/next'
import FuncHead from '../../components/FuncHead'
import AddSysConfigDialog from './AddSysConfigDialog'
import './index.scss'

const FormItem = Form.Item

const dataSource = []

for (let i = 0; i < 10; i++) {
    dataSource.push({
        c1: 'key1',
        c2: '1',
    })
}

@ConfigProvider.config
class SystemConfig extends React.Component {
    static displayName = 'SystemConfig'

    constructor(props) {
        super(props)
        this.addDialog = React.createRef()
    }

    deleteServiceSync() {
        const {locale = {}} = this.props
        Dialog.confirm({
            title: locale.confirm,
            content: locale.confirmMsg,
            onOk: () => setTimeout(() => Message.success(locale.successMsg), 888),
            onCancel: () => console.log('cancel')
        })
    }

    openAddDialog() {
        this.addDialog.current.getInstance().open()
    }

    render() {
        const {locale = {}} = this.props
        return (
            <div className="system-config">
                <FuncHead title={locale.title}/>
                <Form inline className="search-form">
                    <FormItem label={`${locale.configName}:`}>
                        <Input style={{width: 198}} placeholder={locale.configNamePlaceholder}/>
                    </FormItem>
                    <Button type="primary">{locale.search}</Button>
                    <Button type="normal" className="add-btn"
                            onClick={() => this.openAddDialog()}>{locale.addConfig}</Button>
                </Form>
                <Table dataSource={dataSource}>
                    <Table.Column title={locale.configName} dataIndex="c1"/>
                    <Table.Column title={locale.value} dataIndex="c2"/>
                    <Table.Column
                        title={locale.operation}
                        cell={(value, index, record) => (
                            <Button type="primary" text onClick={() => this.deleteServiceSync(record)}
                            >{locale.deleteBtn}</Button>
                        )}
                    />
                </Table>
                <Pagination onChange={f => f} className="list-pagination"/>
                <AddSysConfigDialog ref={this.addDialog}/>
            </div>
        )
    }
}

export default SystemConfig
