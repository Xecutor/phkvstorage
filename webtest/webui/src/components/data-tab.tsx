import * as React from "react";
import * as ReactDOM from "react-dom";

import { Button, List, Input, Segment, Label } from 'semantic-ui-react'
import { jsonrpcCall } from "../utils/jsonrpc";

interface DataItem {
    type: 'dir' | 'key'
    name: string
    value?: string
    children?: DataItem[]
}

interface DataTabState {
    startAt: string
    baseDir?: string
    error?: string
    content?: DataItem
}

interface DataTabProps {
}

export class DataTab extends React.Component<DataTabProps, DataTabState>{
    constructor(props: DataTabProps) {
        super(props)
        this.state = {
            startAt: '/',
        }
    }

    onResult(result: any) {
        let dir = result.dir as string
        let content = result.content as DataItem[]
        if (content instanceof Array) {
            this.setState({ content: { type: 'dir', name: dir, children: content } })
        }
    }
    onResultBound = this.onResult.bind(this)

    onError(reason: any) {
        this.setState({ error: 'Error' + reason })
    }
    onErrorBound = this.onError.bind(this)

    onStartAtChanged(e: any, { value }: { value: string }) {
        this.setState({ startAt: value })
    }
    onStartAtChangedBound = this.onStartAtChanged.bind(this)

    onBrowseClick() {
        this.setState({ baseDir: this.state.startAt })
        this.onRequestData(this.state.startAt)
    }
    onBrowseClickBound = this.onBrowseClick.bind(this)

    onRequestData(dir: string) {
        jsonrpcCall("get_dir_entries", { dir: this.state.startAt }).then(this.onResultBound, this.onErrorBound)
    }

    makeContent() {
        if (this.state.content) {
            return (
                <List>
            {this.state.content.children.map(item=>
                    <List.Item>
                        <List.Icon name={item.type=='dir'?'folder':'file'}/>
                        <List.Content>{item.name}</List.Content>
                    </List.Item>)
            }
                </List>)
        }
        else {
            return (
                <List>
                </List>
            )
        }
    }

    render() {
        let error
        if (this.state.error) {
            error = <Segment><Label color="red">{this.state.error}</Label></Segment>
        }
        return <Segment.Group>
            <Segment><Input label="Start at" value={this.state.startAt} onChange={this.onStartAtChangedBound} action={<Button onClick={this.onBrowseClickBound}>Browse</Button>} /></Segment>
            <Segment>
                {this.makeContent()}
            </Segment>
        </Segment.Group>
    }
}
