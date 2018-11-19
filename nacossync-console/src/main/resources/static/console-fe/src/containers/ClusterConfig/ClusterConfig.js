import React from 'react'
import {connect} from 'react-redux'
import {Table, Pagination, Form, Input, Button, Dialog, Message, ConfigProvider} from '@alifd/next'
import FuncHead from '../../components/FuncHead'
import AddConfigDialog from './AddConfigDialog'
import {deleteCluster, list} from '../../reducers/cluster'
import './index.scss'

const FormItem = Form.Item

@connect(state => ({...state.cluster}), {list})
@ConfigProvider.config
class ClusterConfig extends React.Component {
    constructor(props) {
        super(props)
        this.addDialog = React.createRef()
        this.state = {
            pageNum: 1,
            loading: true,
            search: {
                clusterName: ''
            }
        }
    }

    componentDidMount() {
        const {pageNum} = this.state
        const {list} = this.props
        list({pageNum}).then(() => this.loadComplete())
    }

    loadComplete() {
        this.setState({loading: false})
    }

    deleteServiceSync(record) {
        const {clusterId} = record
        const {locale = {}, clusterModels = []} = this.props
        Dialog.confirm({
            title: locale.confirm,
            content: locale.confirmMsg,
            onOk: () => deleteCluster({clusterId}).then(() => {
                let pageNum = 1
                if (this.state.pageNum > 1) {
                    if (clusterModels.length === 1) {
                        pageNum = this.state.pageNum - 1
                    }
                }
                this.turnPage(pageNum)
                Message.success(locale.successMsg)
            })
        })
    }

    turnPage(pageNum) {
        const {search} = this.state
        const {list} = this.props
        this.setState({pageNum, loading: true}, () => list({pageNum, ...search}).then(() => this.loadComplete()))
    }

    onChangeSearchForm(obj) {
        const {search} = this.state
        this.setState({search: Object.assign({}, search, obj)})
    }

    openAddDialog() {
        this.addDialog.current.getWrappedInstance().getInstance().open()
    }

    render() {
        const {loading, pageNum, search} = this.state
        const {types = [], clusterModels = [], locale = {}, totalSize = 0, totalPage = 0} = this.props

        return (
            <div className="cluster-config">
                <FuncHead title={locale.title}/>
                <Form inline className="search-form">
                    <FormItem label={`${locale.clusterName}:`}>
                        <Input
                            style={{width: 198}}
                            value={search.clusterName}
                            placeholder={locale.clusterNamePlaceholder}
                            onChange={clusterName => this.onChangeSearchForm({clusterName})}
                        />
                    </FormItem>
                    <Button type="primary" onClick={() => this.turnPage(1)}>{locale.search}</Button>
                    <Button
                        type="normal"
                        className="add-btn"
                        onClick={() => this.openAddDialog()}
                    >{locale.addCluster}</Button>
                </Form>
                <Table dataSource={clusterModels} loading={loading}>
                    <Table.Column title={locale.clusterName} dataIndex="clusterName"/>
                    <Table.Column title={locale.clusterType} dataIndex="clusterType"/>
                    <Table.Column title={locale.connectKeyList} dataIndex="connectKeyList"/>
                    <Table.Column
                        title={locale.operation}
                        cell={(value, index, record) => (
                            <Button
                                text
                                type="primary"
                                onClick={() => this.deleteServiceSync(record)}
                            >{locale.deleteBtn}</Button>
                        )}
                    />
                </Table>
                {
                    totalPage > 1
                        ? <Pagination
                            onChange={pageNum => this.turnPage(pageNum)}
                            current={pageNum}
                            total={totalSize}
                            className="list-pagination"
                        />
                        : null
                }
                <AddConfigDialog ref={this.addDialog} types={types} turnPage={pn => this.turnPage(pn)}/>
            </div>
        )
    }
}

export default ClusterConfig
