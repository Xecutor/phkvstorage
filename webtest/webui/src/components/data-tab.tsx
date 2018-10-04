import * as React from "react";
import * as ReactDOM from "react-dom";

import { Button, List, Input, Segment, Label, ListItemProps, Icon } from 'semantic-ui-react'
import { jsonrpcCall } from "../utils/jsonrpc";

interface DataItem {
    type: 'dir' | 'key'
    name: string
    value?: string
    children?: DataItem[]
    expanded?: boolean
    parent?:DataItem
}

interface DataTabState {
    startAt: string
    baseDir?: string
    error?: string
    content?: DataItem
}

interface DataTabProps {
}

function concatPath(basePath: string, item: string) {
    if (basePath[basePath.length - 1] != '/') {
        return basePath + '/' + item;
    }
    else {
        if(item[0]=='/') {
            return basePath + item.substr(1)
        }
        else {
            return basePath + item;
        }
    }
}

function removeDataItemFromParent(item:DataItem) {
    if(item.parent) {
        item.parent.children = item.parent.children.filter(v=>v.name!=item.name)
    }
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
            console.log(`dir=${dir}, basedir=${this.state.baseDir}`)
            let root : DataItem = { type: 'dir', name: dir, expanded: true, children: content }
            for(let c of content){
                c.parent = root
            }
            this.setState({ content: root })
        }
    }
    onResultBound = this.onResult.bind(this)

    onError(reason: any) {
        alert(reason);
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
        jsonrpcCall("get_dir_entries", { dir }).then(this.onResultBound, this.onErrorBound)
    }

    updateContent() {
        let newRoot = {...this.state.content}
        for(let c of newRoot.children) {
            c.parent = newRoot
        }
        this.setState({content:newRoot})
    }

    expandKey(path: string, item: DataItem) {
        console.log("expand key " + concatPath(path, item.name))
        if (typeof (item.value) !== 'undefined') {
            console.log("expand existing")
            item.expanded = !item.expanded
            this.updateContent()
        }
        else {
            console.log(`use lookup path=${path}, item.name=${item.name}`)
            jsonrpcCall("lookup", { key: concatPath(path, item.name) }).then(({ value }) => {
                item.value = value
                item.expanded = true
                this.updateContent()
                console.log("lookup result", value)
            }, this.onErrorBound)
        }
    }

    expandDir(path: string, dir: DataItem) {
        console.log("expand dir " + concatPath(path, dir.name))
        if (dir.children) {
            console.log("expand dir existing");
            dir.expanded = !dir.expanded
            this.updateContent()
        }
        else {
            console.log(`expand dir requesting ${path}, dir.name=${dir.name}`)
            jsonrpcCall("get_dir_entries", { dir: concatPath(path, dir.name) }).then((result: any) => {
                let content = result.content as DataItem[]
                if (content instanceof Array) {
                    for(let c of content) {
                        c.parent = dir
                    }
                    dir.children = content;
                    dir.expanded = true;
                    this.updateContent()
                }
            }, this.onErrorBound)
        }
    }

    onEraseKey(path:string, item:DataItem){
        jsonrpcCall("erase_key",{key:concatPath(path, item.name)}).then(()=>{
            removeDataItemFromParent(item);
            this.updateContent()
        },this.onErrorBound)
    }

    onEraseDirRec(path:string, item:DataItem){
        jsonrpcCall("erase_dir_recursive",{dir:concatPath(path, item.name)}).then(()=>{
            removeDataItemFromParent(item);
            this.updateContent()
        },this.onErrorBound)
    }

    makeListContent(path: string, item: DataItem): JSX.Element {
        if (item) {
            if (item.type == 'key') {
                console.log(`key path=${path}`, item)
                return <List.Item key={item.name} onClick={(event) => {event.stopPropagation();this.expandKey(path, item);}}>
                    <List.Icon name='file alternate' />
                    <List.Content>
                        {item.name}&nbsp;
                        <Icon style={{verticalAlign:'middle'}} name='remove' color='red' inverted circular link size='tiny' 
                            onClick={()=>this.onEraseKey(path,item)}
                        />
                    </List.Content>
                    {item.expanded && typeof (item.value !== 'undefined') &&
                        <List.Header>{item.value}</List.Header>
                    }
                </List.Item>
            }
            else {
                console.log(`dir path=${path}`, item)
                return <List.Item key={item.name} onClick={(event) => {event.stopPropagation();this.expandDir(path, item);}}>
                    <List.Icon name={item.expanded?'folder open':'folder'} />
                    <List.Content>{item.name} &nbsp;
                        <Icon style={{verticalAlign:'middle'}} name='remove' color='red' inverted circular link size='tiny'
                            onClick={()=>this.onEraseDirRec(path,item)}
                        />
                        {
                            item.expanded && item.children &&
                            <List.List>
                                {item.children.map(citem => this.makeListContent(concatPath(path, item.name), citem))}
                            </List.List>
                        }
                    </List.Content>
                </List.Item>
            }
        }
        else {
            return <List.Item></List.Item>
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
                <List>
                    {this.makeListContent('/', this.state.content)}
                </List>
            </Segment>
        </Segment.Group>
    }
}
